#!/usr/bin/env python3
"""Safely sort top-level files in a folder into type-based subfolders."""

from __future__ import annotations

import argparse
import errno
import hashlib
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


CATEGORY_EXTENSIONS: Dict[str, Set[str]] = {
    "video": {
        ".mp4",
        ".mkv",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".m4v",
    },
    "audio": {".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a", ".wma"},
    "image": {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp", ".heic", ".svg"},
    "gif": {".gif"},
    "text": {".txt", ".md", ".rtf", ".csv", ".log"},
    "docs": {".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"},
    "archives": {".zip", ".rar", ".7z", ".tar", ".gz"},
    "code": {
        ".py",
        ".js",
        ".ts",
        ".java",
        ".c",
        ".cpp",
        ".cs",
        ".go",
        ".rs",
        ".json",
        ".xml",
        ".yml",
        ".yaml",
        ".html",
        ".css",
        ".sql",
    },
    "others": set(),
}


EXTENSION_TO_CATEGORY: Dict[str, str] = {}
for category_name, extensions in CATEGORY_EXTENSIONS.items():
    if category_name == "others":
        continue
    for extension in extensions:
        EXTENSION_TO_CATEGORY[extension] = category_name


DEFAULT_HASH_WARN_THRESHOLD_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
COPY_CHUNK_SIZE_BYTES = 8 * 1024 * 1024  # 8 MiB
HASH_CHUNK_SIZE_BYTES = 4 * 1024 * 1024  # 4 MiB


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sort top-level files by extension into type-based subfolders "
            "using safe copy-verify-delete semantics."
        )
    )
    parser.add_argument(
        "folder_paths",
        nargs="+",
        help="One or more folder paths containing files to sort.",
    )
    parser.add_argument(
        "--verify-hash",
        action="store_true",
        help="Use SHA-256 hash verification after copy (slower, stronger).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without copying or deleting files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-file operations and outcomes.",
    )
    return parser.parse_args(argv)


def is_permission_issue(error: OSError) -> bool:
    return isinstance(error, PermissionError) or getattr(error, "errno", None) in {
        errno.EACCES,
        errno.EPERM,
    }


def raise_permission_error(operation: str, path: Path, error: OSError) -> None:
    raise PermissionError(
        f"{operation} failed for '{path}': permission denied "
        f"(administrator permission may be required). Original error: {error}"
    ) from error


def classify_file(file_path: Path) -> Optional[str]:
    return EXTENSION_TO_CATEGORY.get(file_path.suffix.lower())


def sha256_for_file(file_path: Path, chunk_size: int = HASH_CHUNK_SIZE_BYTES) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as file_handle:
        while True:
            data = file_handle.read(chunk_size)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()


def verify_copy(source: Path, destination: Path, verify_hash: bool) -> bool:
    if source.stat().st_size != destination.stat().st_size:
        return False
    if not verify_hash:
        return True
    return sha256_for_file(source) == sha256_for_file(destination)


def files_are_identical(source: Path, existing: Path) -> bool:
    if source.stat().st_size != existing.stat().st_size:
        return False
    return sha256_for_file(source) == sha256_for_file(existing)


def path_key(path: Path) -> str:
    return str(path).lower()


def resolve_destination_path(
    destination_dir: Path, file_name: str, reserved_keys: Set[str]
) -> Path:
    stem = Path(file_name).stem
    suffix = Path(file_name).suffix
    candidate = destination_dir / file_name
    index = 1
    while candidate.exists() or path_key(candidate) in reserved_keys:
        candidate = destination_dir / f"{stem}_{index}{suffix}"
        index += 1
    reserved_keys.add(path_key(candidate))
    return candidate


def copy_file_fragmented(source: Path, destination: Path) -> None:
    temp_path = destination.with_name(destination.name + ".part")
    try:
        if temp_path.exists():
            temp_path.unlink()

        with source.open("rb") as src_handle, temp_path.open("wb") as dst_handle:
            while True:
                chunk = src_handle.read(COPY_CHUNK_SIZE_BYTES)
                if not chunk:
                    break
                dst_handle.write(chunk)
            dst_handle.flush()
            os.fsync(dst_handle.fileno())

        shutil.copystat(source, temp_path, follow_symlinks=True)
        os.replace(temp_path, destination)
    except OSError:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass
        raise


def classify_top_level_files(root_dir: Path) -> Tuple[Dict[str, List[Path]], List[Path], int, int]:
    try:
        entries = list(root_dir.iterdir())
    except OSError as error:
        if is_permission_issue(error):
            raise_permission_error("Read directory", root_dir, error)
        raise

    classified_files: Dict[str, List[Path]] = {category: [] for category in CATEGORY_EXTENSIONS}
    unmatched_files: List[Path] = []
    scanned_files = 0
    matched_total_bytes = 0

    for entry in entries:
        try:
            if not entry.is_file():
                continue
        except OSError as error:
            if is_permission_issue(error):
                raise_permission_error("Inspect entry", entry, error)
            print(f"Skipping unreadable entry '{entry}': {error}", file=sys.stderr)
            continue

        scanned_files += 1
        category = classify_file(entry)
        if category is None:
            unmatched_files.append(entry)
            continue

        classified_files[category].append(entry)
        try:
            matched_total_bytes += entry.stat().st_size
        except OSError as error:
            if is_permission_issue(error):
                raise_permission_error("Read file metadata", entry, error)
            print(f"Skipping metadata read for '{entry}': {error}", file=sys.stderr)

    return classified_files, unmatched_files, scanned_files, matched_total_bytes


def should_continue_after_hash_warning(total_bytes: int) -> bool:
    size_gb = total_bytes / (1024 * 1024 * 1024)
    print(
        "Warning: high input data size detected with --verify-hash "
        f"({size_gb:.2f} GiB). Hash verification may take a long time."
    )
    answer = input("Proceed anyway? (y/n): ").strip().lower()
    return answer == "y"


def sort_files(
    root_dir: Path, verify_hash: bool, dry_run: bool, verbose: bool
) -> int:
    classified_files, unmatched_files, scanned_files, matched_total_bytes = classify_top_level_files(
        root_dir
    )

    detected_categories = [
        category for category in CATEGORY_EXTENSIONS if category != "others" and classified_files[category]
    ]

    if verbose:
        mode_text = "DRY-RUN" if dry_run else "LIVE"
        print(f"Mode: {mode_text}")
        print(f"Root folder: {root_dir}")
        print(f"Top-level files scanned: {scanned_files}")
        print(f"Detected categories: {', '.join(detected_categories) if detected_categories else 'none'}")

    if verify_hash and not dry_run and matched_total_bytes >= DEFAULT_HASH_WARN_THRESHOLD_BYTES:
        if not should_continue_after_hash_warning(matched_total_bytes):
            print("Operation cancelled by user.")
            return 1

    if not dry_run:
        for category in detected_categories:
            destination_dir = root_dir / category
            try:
                destination_dir.mkdir(exist_ok=True)
            except OSError as error:
                if is_permission_issue(error):
                    raise_permission_error("Create subfolder", destination_dir, error)
                raise

    reserved_destination_keys: Set[str] = set()
    copied_files = 0
    deleted_originals = 0
    failed_files = 0
    deduplicated_files = 0

    for category in detected_categories:
        destination_dir = root_dir / category
        for source in classified_files[category]:
            same_name_destination = destination_dir / source.name
            if same_name_destination.exists() and same_name_destination.is_file():
                try:
                    if files_are_identical(source, same_name_destination):
                        if dry_run:
                            deduplicated_files += 1
                            if verbose:
                                print(
                                    f"[DRY-RUN] REDUNDANT '{source.name}' already exists in "
                                    f"'{destination_dir}'. DELETE source only."
                                )
                        else:
                            source.unlink()
                            deduplicated_files += 1
                            deleted_originals += 1
                            if verbose:
                                print(
                                    f"REDUNDANT-DELETED source '{source.name}' "
                                    f"(identical file already in '{destination_dir}')"
                                )
                        continue
                except OSError as error:
                    failed_files += 1
                    if is_permission_issue(error):
                        raise_permission_error("Compare or delete redundant file", source, error)
                    print(
                        f"Failed redundancy handling for '{source}' against "
                        f"'{same_name_destination}': {error}",
                        file=sys.stderr,
                    )
                    continue

            destination = resolve_destination_path(destination_dir, source.name, reserved_destination_keys)
            if dry_run:
                copied_files += 1
                if verbose:
                    print(f"[DRY-RUN] COPY '{source.name}' -> '{destination}'")
                    print(f"[DRY-RUN] DELETE '{source}' after verification")
                continue

            try:
                copy_file_fragmented(source, destination)
            except OSError as error:
                failed_files += 1
                if is_permission_issue(error):
                    raise_permission_error("Copy file", source, error)
                print(f"Failed to copy '{source}' -> '{destination}': {error}", file=sys.stderr)
                continue

            try:
                is_verified = verify_copy(source, destination, verify_hash)
            except OSError as error:
                failed_files += 1
                if is_permission_issue(error):
                    raise_permission_error("Verify copied file", source, error)
                print(
                    f"Failed to verify '{source}' -> '{destination}': {error}",
                    file=sys.stderr,
                )
                continue

            if not is_verified:
                failed_files += 1
                print(
                    f"Verification failed for '{source}' -> '{destination}'. "
                    "Original file was not deleted.",
                    file=sys.stderr,
                )
                continue

            try:
                source.unlink()
            except OSError as error:
                failed_files += 1
                if is_permission_issue(error):
                    raise_permission_error("Delete original file", source, error)
                print(f"Copied but failed to delete original '{source}': {error}", file=sys.stderr)
                continue

            copied_files += 1
            deleted_originals += 1
            if verbose:
                print(f"COPIED+DELETED '{source.name}' -> '{destination}'")

    matched_files = sum(len(classified_files[category]) for category in detected_categories)
    accounted_for = matched_files + len(unmatched_files)

    print("\nSummary")
    print(f"Scanned top-level files: {scanned_files}")
    if dry_run:
        print(f"Planned copies: {copied_files}")
        print("Planned deletions: same as planned copies (only after successful verification)")
    else:
        print(f"Copied successfully: {copied_files}")
        print(f"Deleted originals: {deleted_originals}")
        print(f"Deduplicated (already existed): {deduplicated_files}")
    print(f"Failed files: {failed_files}")
    print(f"Unmatched files (left in root): {len(unmatched_files)}")

    if unmatched_files:
        print("Unmatched file names:")
        for file_path in unmatched_files:
            print(f"- {file_path.name}")

    if accounted_for != scanned_files:
        print(
            "Warning: internal accounting mismatch detected; "
            "some files may not have been considered.",
            file=sys.stderr,
        )
        return 1

    return 1 if failed_files > 0 else 0


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    overall_exit = 0

    for raw_path in args.folder_paths:
        root_dir = Path(raw_path).expanduser()
        print(f"\nProcessing folder: {root_dir}")
        if not root_dir.exists():
            print(f"Error: folder does not exist: '{root_dir}'", file=sys.stderr)
            overall_exit = 2
            continue
        if not root_dir.is_dir():
            print(f"Error: input path is not a folder: '{root_dir}'", file=sys.stderr)
            overall_exit = 2
            continue

        try:
            folder_exit = sort_files(
                root_dir=root_dir,
                verify_hash=args.verify_hash,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            if folder_exit != 0 and overall_exit == 0:
                overall_exit = folder_exit
        except PermissionError as error:
            print(f"Permission error: {error}", file=sys.stderr)
            overall_exit = 2
        except ValueError as error:
            print(f"Value error: {error}", file=sys.stderr)
            overall_exit = 2
        except OSError as error:
            print(f"Unexpected filesystem error: {error}", file=sys.stderr)
            overall_exit = 2

    return overall_exit


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
