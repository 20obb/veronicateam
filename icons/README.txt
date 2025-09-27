Place package icon images here.

Naming: <Package-ID>.<ext>
Examples:
- com.tigisoftware.filza.png
- com.opa334.libsandy.png
- ai.akemi.appsyncunified.webp

Supported extensions: .png, .jpg, .jpeg, .webp

How to apply to Packages:
1) Add or update icons in this folder.
2) Run the updater with icons:
   py -3 tools\update_packages.py --add-icons --verbose

Optional:
- Use --icon-url-prefix "https://example.com/repo/icons" to force absolute URLs if hosting icons on GitHub Pages.
- Or keep default relative paths (Icon: icons/<file>) and ensure the icons/ folder is uploaded next to Packages.
