import os
import time
import requests

RE_TRIAL = 3
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def download(start_year: int, end_year: int, data_dir: str):
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            month_label = f"{year}-{month:02d}"
            filename = f"yellow_tripdata_{month_label}.parquet"
            out_path = os.path.join(data_dir, filename)

            if os.path.exists(out_path):
                print(f"\n[SKIP] {month_label} already exists\n")
                continue

            url = f"{BASE_URL}/{filename}"

            for i in range(RE_TRIAL):
                print(f"\n[DOWNLOAD] {month_label} ...")
                try:
                    resp = requests.get(url, stream=True, timeout=30)
                    resp.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                            f.write(chunk)
                    print(f"  -> saved to {out_path}\n")
                    break
                except Exception as e:
                    print(f"Error at {month_label} - {e}")
                    if os.path.exists(out_path):
                        os.remove(out_path)
                    time.sleep(10)
                    print(f"\nRetry to download {month_label} #{i+1}")
            else:
                print(f"\n[FAIL] {month_label} could not be downloaded\n")

    print("Done!")


if __name__ == "__main__":
    data_dir = "./data"
    os.makedirs(data_dir, exist_ok=True)

    download(start_year=2018, end_year=2019, data_dir=data_dir)
