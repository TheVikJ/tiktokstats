import sys
import json
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

original_argv = sys.argv[:]
sanitized_argv = [original_argv[0]]
skip_next = False
for a in original_argv[1:]:
    if skip_next:
        skip_next = False
        continue
    if a == "-m" or a == "--minvideos" or a.startswith("--minvideos="):
        if a == "-m" or a == "--minvideos":
            skip_next = True
        continue
    sanitized_argv.append(a)
sys.argv = sanitized_argv

from randomsample import (
    ROOT_DIR, initialize_collection, generate_random_timestamp,
    generate_ids_from_timestamp, check_url,
    JAN_1_2018, TIME_NOW
)

sys.argv = original_argv

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--samplesize", type=int)
parser.add_argument("-t", "--threads", type=int)
parser.add_argument("-b", "--begintimestamp", type=int)
parser.add_argument("-e", "--endtimestamp", type=int)
parser.add_argument("-i", "--incrementershortcut", type=int)
parser.add_argument("-m", "--minvideos", type=int)
args = parser.parse_args()

sample_size, threads = 50000, 15
begin_timestamp, end_timestamp = JAN_1_2018, TIME_NOW
incrementer_shortcut, min_videos = 10, 0

if args.samplesize is not None:
    sample_size = args.samplesize
if args.threads is not None:
    threads = args.threads
if args.begintimestamp is not None:
    begin_timestamp = args.begintimestamp
if args.endtimestamp is not None:
    end_timestamp = args.endtimestamp
if args.incrementershortcut is not None:
    if 0 < args.incrementershortcut < 2**6:
        incrementer_shortcut = args.incrementershortcut
    else:
        raise ValueError("invalid incrementer limit")
if args.minvideos is not None:
    min_videos = args.minvideos

collection = f"minrandom_tiktok_i_{incrementer_shortcut}_" + \
             datetime.now().strftime('%Y%m%d_%H%M%S_%f')


def main():
    print(initialize_collection(collection))
    total_hits = 0

    while min_videos == 0 or total_hits < min_videos:
        random_timestamp = generate_random_timestamp(
            start_timestamp=begin_timestamp,
            end_timestamp=end_timestamp
        )

        ids = generate_ids_from_timestamp(
            random_timestamp, n=sample_size,
            limit_incrementer_randomness=incrementer_shortcut
        )

        print(datetime.utcfromtimestamp(random_timestamp))

        queries_dir = os.path.join(ROOT_DIR, "collections", collection, "queries")
        os.makedirs(queries_dir, exist_ok=True)
        with open(os.path.join(queries_dir, f"{random_timestamp}_queries.json"), "w") as f:
            json.dump(ids, f)

        results = []
        with tqdm(total=len(ids)) as pbar:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [executor.submit(
                    check_url,
                    f"https://www.tiktok.com/@/video/{i}"
                ) for i in ids]

                for future in as_completed(futures):
                    r = future.result()
                    results.append(r)
                    pbar.update(1)
                    if r.get("statusCode") == "0":
                        total_hits += 1
                        tqdm.write(json.dumps(r))
                    if min_videos > 0 and total_hits >= min_videos:
                        break

        with open(os.path.join(queries_dir, f"{random_timestamp}_hits.json"), "w") as f:
            json.dump(results, f)

    print(f"\nReached {total_hits} valid videos. Finished.")


if __name__ == "__main__":
    main()
