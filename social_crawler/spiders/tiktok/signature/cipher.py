from typing import ClassVar

"""Bộ mã hoá dùng chung của hai bộ ký TikTok (gnarly.py = X-Gnarly, dynosaur.py = X-Dynosaur).

Là biến thể ChaCha: trạng thái 16 từ 32-bit = 4 hằng + 12 từ khoá, số vòng suy ra từ khoá (5..20). Hai bộ ký trước đây chép
nguyên lớp này dưới hai tên (`sexer`, `crypto` - AST giống hệt, kiểm 17/11/2026); gộp lại để một chỗ sửa. KHÔNG đổi thuật toán:
sai một bit là TikTok trả 200 body rỗng.
"""


class ChachaCipher:
    """Biến thể ChaCha dùng chung của hai bộ ký TikTok: trạng thái 16 từ 32-bit (4 hằng + 12 từ khoá), số vòng 5..20
    suy ra từ khoá; `block` sinh khối keystream, `encrypt` mã hoá dữ liệu."""

    const: ClassVar[list[int]] = [1196819126, 600974999, 3863347763, 1451689750]

    def __init__(self, key):
        self.key = [w & 0xFFFFFFFF for w in key]
        acc = 0
        for w in self.key:
            acc = (acc + (w & 0x0F)) & 0x0F
        self.rounds = acc + 5
        self.state = self.const + self.key

    @staticmethod
    def rotl(x, n):
        return ((x << n) | (x >> (32 - n))) & 0xFFFFFFFF

    @classmethod
    def quarter(cls, s, a, b, c, d):
        s[a] = (s[a] + s[b]) & 0xFFFFFFFF
        s[d] = cls.rotl(s[d] ^ s[a], 16)
        s[c] = (s[c] + s[d]) & 0xFFFFFFFF
        s[b] = cls.rotl(s[b] ^ s[c], 12)
        s[a] = (s[a] + s[b]) & 0xFFFFFFFF
        s[d] = cls.rotl(s[d] ^ s[a], 8)
        s[c] = (s[c] + s[d]) & 0xFFFFFFFF
        s[b] = cls.rotl(s[b] ^ s[c], 7)

    @classmethod
    def block(cls, state, rounds):
        w = state.copy()
        r = 0
        while r < rounds:
            cls.quarter(w, 0, 4, 8, 12)
            cls.quarter(w, 1, 5, 9, 13)
            cls.quarter(w, 2, 6, 10, 14)
            cls.quarter(w, 3, 7, 11, 15)
            r += 1
            if r >= rounds:
                break
            cls.quarter(w, 0, 5, 10, 15)
            cls.quarter(w, 1, 6, 11, 12)
            cls.quarter(w, 2, 7, 12, 13)
            cls.quarter(w, 3, 4, 13, 14)
            r += 1
        return [(w[i] + state[i]) & 0xFFFFFFFF for i in range(16)]

    def encrypt(self, data: bytes) -> bytes:
        buf = bytearray(data)
        n_full = len(buf) // 4
        rem = len(buf) % 4
        words = [0] * ((len(buf) + 3) // 4)

        for i in range(n_full):
            j = 4 * i
            words[i] = buf[j] | (buf[j + 1] << 8) | (buf[j + 2] << 16) | (buf[j + 3] << 24)
        if rem:
            for c in range(rem):
                words[n_full] |= buf[4 * n_full + c] << (8 * c)

        state = self.state.copy()
        off = 0
        while off + 16 < len(words):
            stream = self.block(state, self.rounds)
            state[12] = (state[12] + 1) & 0xFFFFFFFF
            for k in range(16):
                words[off + k] ^= stream[k]
            off += 16
        stream = self.block(state, self.rounds)
        for k in range(len(words) - off):
            words[off + k] ^= stream[k]

        for i in range(n_full):
            j = 4 * i
            w = words[i]
            buf[j] = w & 0xFF
            buf[j + 1] = (w >> 8) & 0xFF
            buf[j + 2] = (w >> 16) & 0xFF
            buf[j + 3] = (w >> 24) & 0xFF
        if rem:
            w = words[n_full]
            for c in range(rem):
                buf[4 * n_full + c] = (w >> (8 * c)) & 0xFF
        return bytes(buf)
