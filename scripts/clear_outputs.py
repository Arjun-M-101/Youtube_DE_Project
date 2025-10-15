import os, shutil

for layer in ["silver", "gold"]:
    if os.path.exists(layer):
        for item in os.listdir(layer):
            item_path = os.path.join(layer, item)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f"🧹 Cleared contents of {layer}/ (folder preserved)")
    else:
        os.makedirs(layer)
        print(f"📂 Created missing folder: {layer}/")