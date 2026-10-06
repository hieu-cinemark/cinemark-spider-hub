"""
Spider comment Facebook không bao giờ mở trình duyệt: lấy comment của một bài qua
curl_cffi, dùng query mà `bootstrap_comments()`
(social_crawler.spiders.facebook.auth.bootstrap) đã cache.

Truyền -a dedupe=false để tắt khử trùng giữa các lượt chạy (ví dụ để lấy lại comment đã
thấy ở lượt trước) - mặc định bật mỗi khi kết nối được Redis, và lặng lẽ quay về chỉ khử
trùng trong lượt chạy nếu không.

Chạy:
    scrapy crawl facebook_comments -a post_id="122197539992842674" -a max_pages=3
"""

from __future__ import annotations

import asyncio

import scrapy

from social_crawler.clients.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.facebook import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.graphql_client import (
    CheckpointRequiredError,
    FacebookGraphQLClient,
    RateLimitedError,
    SessionExpiredError,
)
from social_crawler.spiders.facebook.features.comments.extract import (
    comment_post_id,
    extract_comments,
    extract_replies,
    find_comments_page_info,
    find_replies_page_info,
)
from social_crawler.spiders.facebook.items import FacebookCommentItem

logger = get_logger(__name__)

# Trần số *trang* reply mà chuỗi reply của một comment cấp một sẽ duyệt qua (mỗi trang một
# request GraphQL) - tách khỏi max_pages của vòng comment cấp một, không dùng lại nó: một
# bài có thể có nhiều comment cấp một, và mỗi comment có replies_count>0 đều có lời gọi
# _fetch_replies riêng, nên cái này giới hạn chi phí theo từng comment chứ không theo từng
# bài. Nâng từ 3 ban đầu (2026-09-15) lên một trần an toàn cao hơn nhiều (2026-09-16, yêu
# cầu rõ "lấy mọi comment, không giới hạn") - cùng vai trò với max_pages=100 "trần an toàn,
# không phải mục tiêu" của tìm kiếm Facebook (xem comment của spider đó):
# find_replies_page_info/get_replies_next_page vốn tự dừng ngay khi has_next_page là false,
# nên cái này chỉ có ý nghĩa với một chuỗi reply thật sự khổng lồ, và tồn tại chỉ để một bug
# page_info không thể quay vòng mãi chứ không phải để giới hạn độ đầy đủ thật.
MAX_REPLY_PAGES = 200


class FacebookCommentsSpider(scrapy.Spider):
    name = "facebook_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        post_id: str | None = None,
        count: int = -1,
        max_pages: int = 1,
        dedupe: str = "true",
        account: str | None = None,
        include_replies: str = "false",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.post_id = post_id
        # -1 = trang CommentsListComponentsPaginationQuery dày nhất của Comet (mặc định của trình
        # duyệt). Giá trị dương vẫn chạy nhưng Facebook thường bỏ qua và vẫn trả khoảng 10.
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        # Mặc định tắt: mỗi comment cấp một có replies_count>0 từng bắn thêm một lượt GraphQL, và
        # đường query reply hiện trả về edges rỗng với phần lớn tài khoản - đốt ngân sách bóp nhịp
        # mà không ra reply nào. Truyền include_replies=true khi đã sửa được việc bắt đó.
        self.include_replies = str(include_replies).lower() not in ("false", "0", "no")
        # Được crawl_request_consumer.py ghim vào tài khoản nào mà nó vừa xác minh (hoặc vừa
        # bootstrap) có cache query comment dùng được - xem comment của nó để biết vì sao không
        # được quay về mặc định của FacebookGraphQLClient là tự đọc ACTIVE_ACCOUNT_REDIS_KEY (key
        # đó có thể đổi giữa lúc consumer kiểm tra và lúc tiến trình con này thực sự bắt đầu). Chỉ
        # là None với một lần chạy tay `scrapy crawl facebook_comments` từ CLI, khi đó đọc thẳng
        # ACTIVE_ACCOUNT_REDIS_KEY là lựa chọn đúng, duy nhất.
        self.account = account
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

    async def _fetch_replies(self, client: FacebookGraphQLClient, comment_id: str, legacy_comment_id: str):
        """Lấy và yield reply của một comment cấp một, phân trang sâu tối đa MAX_REPLY_PAGES qua
        get_replies_next_page/find_replies_page_info - giống vòng comment cấp một trong start()
        bên dưới, chỉ là gốc đặt ở một comment thay vì một bài."""
        cursor: str | None = None
        page = 1
        total = 0
        while True:
            try:
                if cursor is None:
                    response = await asyncio.to_thread(client.get_replies, legacy_comment_id)
                else:
                    response = await asyncio.to_thread(
                        client.get_replies_next_page, legacy_comment_id, cursor, self.count
                    )
            except SessionExpiredError as exc:
                if page == 1:
                    logger.error(
                        "session_expired",
                        error=str(exc),
                        hint='python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url>" --type replies',
                    )
                else:
                    # get_replies_next_page raise đúng lỗi này khi chưa bắt được query reply *có phân trang* nào
                    # (xem docstring của nó) - khác với việc session của trang 1 thực sự chết, vì get_replies
                    # (trang 1) vừa thành công. Dừng ở đây thay vì huỷ cả comment (trang đầu của nó đã yield
                    # rồi); thứ còn thiếu là chạy bootstrap.py --type replies với một comment có nhiều reply hơn
                    # mức vừa một trang.
                    logger.warning("replies_pagination_not_bootstrapped", parent_comment_id=comment_id, error=str(exc))
                return
            except CheckpointRequiredError as exc:
                # _run() đã tắt tài khoản và gửi cảnh báo Telegram (xem comet_graphql_client.py).
                logger.error("checkpoint_required", error=str(exc))
                return
            except RateLimitedError as exc:
                logger.error("rate_limited", telegram=True, error=str(exc))
                return

            replies = extract_replies(response)
            for reply in replies:
                reply_id = reply.get("comment_id")
                if not reply_id:
                    logger.warning("reply_missing_id", parent_comment_id=comment_id)
                    continue
                if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, reply_id) == 0:
                    continue
                published = await self._kafka.publish(
                    topic=RAW_COMMENTS_TOPIC,
                    key=f"facebook:{reply_id}",
                    value={
                        "platform": "facebook",
                        "post_id": self.post_id,
                        "parent_comment_id": comment_id,
                        **reply,
                    },
                )
                if not published:
                    # Chưa tới Kafka: gỡ dấu "đã thấy" để lượt crawl sau thử lại, giống spider bài viết.
                    if self._cache:
                        self._cache.srem(SEEN_COMMENTS_KEY, reply_id)
                    continue
                total += 1
                yield FacebookCommentItem(post_id=self.post_id, parent_comment_id=comment_id, **reply)

            logger.info("replies_page_crawled", parent_comment_id=comment_id, page=page, new_replies=len(replies))

            # Docstring của find_replies_page_info đánh dấu đường dẫn response của nó là phỏng đoán tốt
            # nhất chưa xác nhận (chưa từng thấy response reply nhiều trang thật) - đóng an toàn ở đây
            # (coi page_info thiếu/rỗng là "không còn trang" thay vì lỗi) nghĩa là đoán sai cũng không
            # tệ hơn hành vi một trang ban đầu, không gây crash.
            page_info = find_replies_page_info(response)
            if page >= MAX_REPLY_PAGES or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1

        logger.info("replies_fetched", parent_comment_id=comment_id, pages=page, count=total)

    async def start(self):
        if not self.post_id:
            logger.error("missing_post_id", hint='scrapy crawl facebook_comments -a post_id="<a post id>"')
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            # Dùng lại RedisCache/connection của chính spider này thay vì để client tự mở thêm một cái
            # thứ hai, độc lập bên trong.
            client = FacebookGraphQLClient(redis_cache=self._cache, account=self.account)
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc))
            return
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            return

        cursor: str | None = None
        page = 1
        total_count = 0

        try:
            while True:
                try:
                    if cursor is None:
                        response = await asyncio.to_thread(client.get_comments, self.post_id)
                    else:
                        response = await asyncio.to_thread(
                            client.get_comments_next_page, self.post_id, cursor, self.count
                        )
                except SessionExpiredError as exc:
                    logger.error(
                        "session_expired",
                        error=str(exc),
                        hint='python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url>"',
                    )
                    return
                except CheckpointRequiredError as exc:
                    # _run() đã tắt tài khoản và gửi cảnh báo Telegram (xem comet_graphql_client.py).
                    logger.error("checkpoint_required", error=str(exc))
                    return
                except RateLimitedError as exc:
                    logger.error("rate_limited", telegram=True, error=str(exc))
                    return

                comments = extract_comments(response)
                # Mọi id comment đều mã hoá bài mà nó thuộc về. Một query comment đã cache mà bỏ qua
                # post_id (bắt được 2026-09-26: một query trình xem media dùng key initial_node_id cố định)
                # trả về comment của một bài KHÁC cho mọi request - publish chúng sẽ gắn chúng vào sai bài.
                owners = {comment_post_id(c.get("comment_id")) for c in comments} - {None}
                if owners and str(self.post_id) not in owners:
                    logger.error(
                        "comments_query_wrong_target",
                        telegram=True,
                        post_id=self.post_id,
                        returned_comments_of=sorted(owners)[:3],
                        page=page,
                        hint="the cached comments query ignores post_id - re-run "
                        'python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a regular post URL>"',
                    )
                    return
                new_count = 0
                for comment in comments:
                    comment_id = comment.get("comment_id")
                    if not comment_id:
                        logger.warning("comment_missing_id", post_id=self.post_id)
                        continue
                    # Giá trị trả về của sadd() vốn đã trả lời "cái này có mới không" trong một lượt nguyên tử
                    # - không cần kiểm tra sismember riêng (và không có cuộc đua giữa lần kiểm tra và lần add
                    # sau đó).
                    if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
                        continue
                    published = await self._kafka.publish(
                        topic=RAW_COMMENTS_TOPIC,
                        key=f"facebook:{comment_id}",
                        # Dict của extract_comments không có post_id riêng (nó theo từng comment, không theo từng
                        # response) - không có dòng này, mọi message raw_comments thiếu đúng trường mà consumer cần
                        # để biết comment thuộc bài nào (đã xảy ra thật: ingest_consumer của cinemark-api log
                        # comment_unknown_post post_id=None cho mọi comment). FacebookCommentItem bên dưới vốn đã
                        # đúng (post_id=self.post_id truyền rõ ràng); dòng này chỉ đưa payload Kafka về ngang bằng.
                        value={"platform": "facebook", "post_id": self.post_id, **comment},
                    )
                    if not published:
                        # Chưa tới Kafka: gỡ dấu "đã thấy" để lượt crawl sau thử lại, giống spider bài viết.
                        if self._cache:
                            self._cache.srem(SEEN_COMMENTS_KEY, comment_id)
                        continue
                    new_count += 1
                    total_count += 1
                    yield FacebookCommentItem(post_id=self.post_id, **comment)

                    if self.include_replies and comment.get("replies_count") and comment.get("legacy_comment_id"):
                        async for reply_item in self._fetch_replies(client, comment_id, comment["legacy_comment_id"]):
                            total_count += 1
                            yield reply_item

                logger.info("page_crawled", page=page, new_comments=new_count, fetched=len(comments))

                page_info = find_comments_page_info(response)
                if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                    break
                cursor = page_info.get("end_cursor")
                if not cursor:
                    break
                page += 1
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, post_id=self.post_id, comments=total_count, pages=page)
