# %%
import os


def delete_all_files(folder_path="../data"):
    print(f"Deleting all files in {folder_path}...")
    for root, _, files in os.walk(folder_path):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"- {file_path} deleted successfully")
            except Exception as e:
                print(f"Error deleting {file}: {e}")
    print("Files deleted.")

delete_all_files()
