import os
import json
import shutil
import argparse
from os.path import expanduser
from tiktoktools import ROOT_DIR
from tiktoktools.metadata import analyze_collection

parser = argparse.ArgumentParser()
parser.add_argument("collection", type=str, help="collection to analyze")
parser.add_argument("-i", "--incrementer-shortcut", action="store_true",)
args = parser.parse_args()

collection = args.collection  # "random_tiktok_20240706_190713_720518"  # wells
collection_address = str(os.path.join(ROOT_DIR, "collections", collection))
unified_collection_folder = "tiktok-random"
if args.incrementer_shortcut:
    unified_collection_folder += "-i"
unified_collection_address = str(os.path.join(expanduser("~"), unified_collection_folder))

os.makedirs(os.path.join(unified_collection_address, "metadata"), exist_ok=True)
os.makedirs(os.path.join(unified_collection_address, "queries"), exist_ok=True)

sampled_seconds = analyze_collection(collection)
print("timestamp, # hits, estimated uploads/s, # private/deleted, estimated uploads/s (+deleted)")
for sampled_second in sampled_seconds:
    metadata_dir = os.path.join(unified_collection_address, "metadata")
    if not os.path.isdir(metadata_dir):
        continue  # Skip if metadata dir is missing
    for hit in [hit for hit in sampled_second["hits"] if f"{hit}.json" not in os.listdir(metadata_dir)]:
        src = os.path.join(collection_address, "metadata", f"{hit}.json")
        dst = os.path.join(metadata_dir, f"{hit}.json")
        if os.path.exists(src):  # Skip if source file doesn't exist
            shutil.copy(src, dst)
    # Ensure queries folder exists before writing
    queries_dir = os.path.join(unified_collection_address, "queries")
    os.makedirs(queries_dir, exist_ok=True)
    with open(os.path.join(queries_dir, f"{sampled_second['timestamp']}.json"), "w") as f:
        json.dump(sampled_second, f, indent=4)
    print(
        f"{sampled_second['timestamp']}, {len(sampled_second['hits'])}, {sampled_second['estimated_uploads_per_second']}, {len(sampled_second['other_messages'])}, {sampled_second['estimated_uploads_all']}")

    error_summary = {}
    if len(sampled_second["error_messages"]) > 0:
        for error_message in sampled_second["error_messages"]:
            if error_message["statusMsg"] not in error_summary:
                error_summary[error_message["statusMsg"]] = 0
            error_summary[error_message["statusMsg"]] += 1
        print(f"{len(sampled_second['error_messages'])} errors")
        print(json.dumps(error_summary, indent=4))

