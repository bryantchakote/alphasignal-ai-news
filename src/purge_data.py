# %%
import os


def delete_all_files(folder_path="../data"):
    for root, _, files in os.walk(folder_path):
        for file in files:
            try:
                os.remove(os.path.join(root, file))
            except Exception as e:
                print(f"Error deleting {file}: {e}")


delete_all_files()
