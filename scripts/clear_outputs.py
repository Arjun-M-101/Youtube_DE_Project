import os, shutil

for layer in ["silver", "gold"]:
    if os.path.exists(layer):
        for item in os.listdir(layer):
            if item == ".gitkeep":   # 👈 skip the placeholder
                continue

            item_path = os.path.join(layer, item)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

        print(f"🧹 Cleared contents of {layer}/ (kept .gitkeep)")
    else:
        os.makedirs(layer)
        # also drop in a .gitkeep if you want it created automatically
        open(os.path.join(layer, ".gitkeep"), "a").close()
        print(f"📂 Created missing folder: {layer}/ with .gitkeep")