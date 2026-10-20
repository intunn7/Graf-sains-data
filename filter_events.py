
from pathlib import Path
import pandas as pd

# Lokasi file berdasarkan folder proyek
DATA_DIR = Path("data")
INPUT_FILE = DATA_DIR / "events.csv"

TRAIN_FILE = DATA_DIR / "events_train.csv"
TEST_FILE = DATA_DIR / "events_test.csv"

# Periode UTS
TRAIN_START = pd.Timestamp("2015-05-11", tz="UTC")
TRAIN_END = pd.Timestamp("2015-07-12", tz="UTC")  # batas akhir eksklusif

TEST_START = pd.Timestamp("2015-07-20", tz="UTC")
TEST_END = pd.Timestamp("2015-08-05", tz="UTC")    # batas akhir eksklusif

# Pastikan file sumber tersedia
if not INPUT_FILE.exists():
    raise FileNotFoundError(f"File tidak ditemukan: {INPUT_FILE}")

# Filter per bagian agar penggunaan memori lebih terkendali
chunk_size = 200_000
train_count = 0
test_count = 0

for chunk in pd.read_csv(INPUT_FILE, chunksize=chunk_size):
    # Timestamp sumber menggunakan Unix epoch milidetik
    waktu = pd.to_datetime(
        chunk["timestamp"], unit="ms", utc=True
    )

    mask_train = (waktu >= TRAIN_START) & (waktu < TRAIN_END)
    mask_test = (waktu >= TEST_START) & (waktu < TEST_END)

    # Simpan header hanya pada bagian pertama
    chunk.loc[mask_train].to_csv(
        TRAIN_FILE,
        mode="a" if train_count else "w",
        header=(train_count == 0),
        index=False,
    )

    chunk.loc[mask_test].to_csv(
        TEST_FILE,
        mode="a" if test_count else "w",
        header=(test_count == 0),
        index=False,
    )

    train_count += int(mask_train.sum())
    test_count += int(mask_test.sum())

print("Filter selesai.")
print(f"Jumlah data training: {train_count:,}")
print(f"Jumlah data testing : {test_count:,}")
print(f"File training: {TRAIN_FILE}")
print(f"File testing : {TEST_FILE}")
