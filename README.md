# file_organizer

## What This Program Is For
`file_organizer` safely organizes mixed files in one or more folders into type-based subfolders using file extensions.

It is useful when a dataset has become messy due to bulk collection, export, or scraping workflows.

## Use Case
This tool is especially useful for cleaning and organizing data that was randomly downloaded with a web crawler before AI model training.

For example, a crawl output folder may contain text, images, videos, archives, and code files mixed together. This script separates them into clear categories so downstream preprocessing and training pipelines can work on cleaner inputs.

## How To Run
### Requirements
- Python 3.8+

### Command
```powershell
python file_organizer.py <folder_path> [more_folder_paths...] [--verify-hash] [--dry-run] [--verbose]
```

Recommended for real runs (safer verification):
```powershell
python file_organizer.py <folder_path> --verify-hash --verbose
```

### Examples
Dry run (no file changes):
```powershell
python file_organizer.py "C:\data\crawl_dump" --dry-run --verbose
```

Live run with stronger verification:
```powershell
python file_organizer.py "C:\data\crawl_dump" --verify-hash --verbose
```

Multiple folders in one run:
```powershell
python file_organizer.py "C:\data\batch1" "C:\data\batch2" --verbose --verify-hash
```

## How It Works
1. Accepts one or more input folders.
2. Scans only top-level files in each folder.
3. Leaves existing subfolders and nested files unchanged.
4. Detects file category by extension (`video`, `audio`, `image`, `gif`, `text`, `docs`, `archives`, `code`).
5. Creates only the category subfolders that are needed.
6. Copies each file to the matching subfolder using a safe chunked temp-file flow.
7. Handles filename collisions by appending numeric suffixes (`_1`, `_2`, ...).
8. Verifies copied files (size check by default, SHA-256 with `--verify-hash`).
9. Deletes original files only after successful verification.
10. Leaves unknown extensions in the root folder and reports them.
11. On permission issues, raises explicit errors.

## Data Safety Notes
- Designed for data-loss prevention first.
- Uses copy-verify-delete instead of direct move.
- If a file is not verified as copied, the original is not deleted.

## Expected Outcome (Successful Run)
- The script prints a summary for each processed folder.
- Detected file types are placed into matching subfolders (`video`, `audio`, `image`, `gif`, `text`, `docs`, `archives`, `code`).
- Original top-level files are deleted only after successful copy and verification.
- Unmatched files remain in the root folder and are listed in output.
- `Failed files` is `0`.

## Expected Outcome (Unsuccessful Run)
- The script prints an explicit error (for example permission denied, verification failure, copy failure, or invalid path).
- Files that fail copy/verification are not deleted from their original location.
- `Failed files` is greater than `0`, or the command exits with a non-zero code.
- If `--verify-hash` is used on large input, a warning is shown and the run may be cancelled by entering `n`.
- If errors persist, re-run with `--verbose` and share the terminal output for troubleshooting.
