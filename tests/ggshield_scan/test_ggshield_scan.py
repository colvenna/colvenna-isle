"""Seams of the publication secret scan (scripts/ggshield_scan.py) that run without ggshield or the network."""

import gzip
import io
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import ggshield_scan  # noqa: E402


def zip_bytes(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def stored_files(out):
    return sorted(path.read_bytes() for path in Path(out).rglob("*") if path.is_file())


class UnpackTest(unittest.TestCase):
    def test_nested_archives_are_opened_and_each_content_is_stored_once(self):
        wheel = zip_bytes({"pkg/settings.py": b"token = 'inner'\n"})
        control = zip_bytes({"handler.py": b"print('handler')\n", "deps/pkg-1.0-py3-none-any.whl": wheel})
        with tempfile.TemporaryDirectory() as work:
            first = Path(work, "colvenna-isle-a.zip")
            second = Path(work, "colvenna-isle-b.zip")
            first.write_bytes(zip_bytes({"control/control.zip": control, "build.log": b"same log\n"}))
            second.write_bytes(zip_bytes({"control/control.zip": control, "build.log": b"same log\n"}))
            out = Path(work, "unpacked")

            result = ggshield_scan.unpack([first, second], out)

            self.assertEqual(stored_files(out), [b"print('handler')\n", b"same log\n", b"token = 'inner'\n"])
            self.assertEqual(result.files, 6)
            self.assertEqual(result.distinct, 3)

    def test_member_names_never_place_a_file_outside_the_output(self):
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w:gz") as archive:
            for name, data in {"/etc/from-tar.txt": b"absolute\n", "../../tar-escape.txt": b"tar up\n"}.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        with tempfile.TemporaryDirectory() as work:
            source = Path(work, "release.zip")
            source.write_bytes(zip_bytes({"../../zip-escape.txt": b"zip up\n", "bundle.tar.gz": tar_buffer.getvalue()}))
            out = Path(work, "nested", "unpacked")

            ggshield_scan.unpack([source], out)

            written = [path for path in Path(work).rglob("*") if path.is_file() and path != source]
            self.assertEqual(sorted(path.read_bytes() for path in written), [b"absolute\n", b"tar up\n", b"zip up\n"])
            for path in written:
                self.assertTrue(path.resolve().is_relative_to(out.resolve()), path)

    def test_a_directory_source_contributes_every_file_beneath_it(self):
        with tempfile.TemporaryDirectory() as work:
            build = Path(work, "colvenna-isle-artifacts")
            (build / "wheel").mkdir(parents=True)
            (build / "validation.log").write_bytes(b"checks passed\n")
            (build / "wheel" / "colvenna_isle-0.0.0-py3-none-any.whl").write_bytes(
                zip_bytes({"colvenna_isle/__init__.py": b"VERSION = '0'\n"}))
            out = Path(work, "unpacked")

            result = ggshield_scan.unpack([build], out)

            self.assertEqual(stored_files(out), [b"VERSION = '0'\n", b"checks passed\n"])
            self.assertEqual(result.files, 2)


    def test_text_over_the_part_size_is_stored_in_overlapping_parts_and_gzip_is_opened(self):
        marker = "BEGIN-" + "x" * 30 + "-END"
        text = ("a" * 95 + marker + "b" * 160).encode()  # the marker crosses the first 100-character cut
        binary = bytes(range(256)) * 2  # not valid UTF-8, like any media file
        with tempfile.TemporaryDirectory() as work:
            Path(work, "models.json.gz").write_bytes(gzip.compress(text))
            Path(work, "promo.mp4").write_bytes(binary)
            out = Path(work, "unpacked")

            result = ggshield_scan.unpack([Path(work, "models.json.gz"), Path(work, "promo.mp4")], out,
                                        part_chars=100, overlap_chars=50)

            stored = stored_files(out)
            parts = [data for data in stored if data != binary]
            self.assertIn(binary, stored)
            self.assertTrue(all(len(part) <= 100 for part in parts))
            self.assertTrue(any(marker.encode() in part for part in parts))
            self.assertGreater(len(parts), 2)
            self.assertEqual(result.files, 2)


    def test_text_under_an_extension_ggshield_drops_is_renamed_and_real_binaries_are_counted(self):
        with tempfile.TemporaryDirectory() as work:
            Path(work, "logo.svg").write_bytes(b"<svg><!-- token: abc --></svg>\n")
            Path(work, "photo.png").write_bytes(bytes(range(256)))
            Path(work, "broken.gz").write_bytes(b"\x1f\x8b\x08\x00" + bytes(range(256)))
            Path(work, "broken.zip").write_bytes(b"PK\x03\x04" + bytes(range(256)))
            out = Path(work, "unpacked")

            result = ggshield_scan.unpack([Path(work)], out, skip_extensions={"svg", "png", "gz", "zip"})

            names = sorted(path.name for path in out.rglob("*") if path.is_file())
            self.assertEqual(names, ["broken.gz", "broken.zip", "logo.svg.txt", "photo.png"])
            self.assertEqual(result.files, 4)
            self.assertEqual(result.unread, 3)

    def test_each_part_records_the_line_it_starts_at(self):
        text = "".join(f"line {number}\n" for number in range(1, 41)).encode()
        with tempfile.TemporaryDirectory() as work:
            Path(work, "big.json").write_bytes(text)
            out = Path(work, "unpacked")

            ggshield_scan.unpack([Path(work, "big.json")], out, part_chars=100, overlap_chars=20)

            original = text.decode().splitlines()
            stored = sorted(out.rglob("big.json"))
            self.assertGreater(len(stored), 2)
            for part in stored:
                place, first = ggshield_scan.file_line(part, 1)
                self.assertTrue(original[first - 1].endswith(part.read_text().splitlines()[0]), part)
                self.assertEqual(ggshield_scan.file_line(part, 3)[1], first + 2)
                self.assertTrue(place.endswith("/big.json"), place)


class ForcePushedHeadsTest(unittest.TestCase):
    def test_lists_each_replaced_head_once_across_pages(self):
        def page(nodes):
            return {"data": {"repository": {"pullRequests": {"nodes": nodes}}}}

        pages = [
            page([{"number": 3, "timelineItems": {"nodes": [
                {"beforeCommit": {"oid": "a" * 40}},
                {"beforeCommit": None},
            ]}}, {"number": 4, "timelineItems": {"nodes": []}}]),
            page([{"number": 9, "timelineItems": {"nodes": [
                {"beforeCommit": {"oid": "d" * 40}},
                {"beforeCommit": {"oid": "a" * 40}},
            ]}}]),
        ]

        self.assertEqual(ggshield_scan.force_pushed_heads(pages), ["a" * 40, "d" * 40])


SECRET = "s3cr3t-Value-0123456789abcdef"


def incident(detector, ignore_sha, line):
    return {"type": detector, "ignore_sha": ignore_sha, "validity": "no_checker", "total_occurrences": 1,
            "occurrences": [{"match": SECRET, "type": "apikey", "line_start": line, "line_end": line,
                             "index_start": 10, "index_end": 39}]}


class SummarizeTest(unittest.TestCase):
    def test_no_incident_in_any_scope_is_clean(self):
        scans = [
            ggshield_scan.Scan("history", 0, {"type": "commit-range", "total_incidents": 0, "scans": []}),
            ggshield_scan.Scan("release assets", 0, {"type": "path_scan", "total_incidents": 0,
                                                   "entities_with_incidents": []}),
        ]

        text, clean = ggshield_scan.summarize(scans)

        self.assertTrue(clean)
        self.assertIn("history: 0 incidents", text)
        self.assertIn("release assets: 0 incidents", text)
        self.assertTrue(text.rstrip().endswith("VERDICT: CLEAN"))

    def test_findings_are_located_without_their_values(self):
        history = {"type": "commit-range", "total_incidents": 1, "scans": [
            {"id": "c" * 40, "type": "commit", "total_incidents": 1, "entities_with_incidents": [
                {"mode": "FILE", "filename": "src/settings.py", "total_incidents": 1,
                 "incidents": [incident("Generic High Entropy Secret", "1" * 64, 12)]}]}]}
        artifacts = {"type": "path_scan", "total_incidents": 1, "entities_with_incidents": [
            {"mode": "FILE", "filename": "/tmp/unpacked/0123456789abcdef/build.log", "total_incidents": 1,
             "incidents": [incident("Generic Password", "2" * 64, 3)]}]}

        text, clean = ggshield_scan.summarize([ggshield_scan.Scan("history", 1, history),
                                             ggshield_scan.Scan("artifacts", 1, artifacts)])

        self.assertFalse(clean)
        self.assertNotIn(SECRET, text)
        self.assertIn("history: 1 incidents", text)
        self.assertIn("Generic High Entropy Secret, commit cccccccccccc, src/settings.py:12, ignore_sha " + "1" * 64,
                      text)
        self.assertIn("Generic Password, 0123456789abcdef/build.log:3, ignore_sha " + "2" * 64, text)
        self.assertTrue(text.rstrip().endswith("VERDICT: FINDINGS"))

    def test_an_incomplete_scan_is_not_clean_even_when_ggshield_exits_0(self):
        failed_chunk = ggshield_scan.Scan("history", 0, {"type": "commit-range", "total_incidents": 0, "scans": []},
                                        "Error: Scanning failed. Results may be incomplete.\n")
        report_errors = ggshield_scan.Scan("paths", 0, {"type": "path_scan", "total_incidents": 0,
                                                      "errors": [{"files": [], "description": "timeout"}]})

        for scan in (failed_chunk, report_errors):
            text, clean = ggshield_scan.summarize([scan])
            self.assertFalse(clean, scan.scope)
            self.assertIn(f"{scan.scope}: incomplete", text)
            self.assertTrue(text.rstrip().endswith("VERDICT: FINDINGS"))

    def test_files_skipped_for_size_are_listed_and_binary_ones_only_counted(self):
        stderr = ("Skipped file:///tmp/unpacked/0123456789abcdef/promo.mp4: can't detect encoding\n"
                  "Warning: Rate-limit hit, retrying in 1 seconds\n"
                  "Skipped file:///tmp/unpacked/fedcba9876543210/big.json: content is over 1,048,576 bytes\n")
        binary_only = ggshield_scan.Scan("paths", 0, {"type": "path_scan", "total_incidents": 0},
                                       stderr.splitlines(keepends=True)[0])
        oversized = ggshield_scan.Scan("paths", 0, {"type": "path_scan", "total_incidents": 0}, stderr)

        text, clean = ggshield_scan.summarize([binary_only])
        self.assertTrue(clean)
        self.assertIn("paths: 0 incidents; 1 binary files not scanned (can't detect encoding)", text)

        text, clean = ggshield_scan.summarize([oversized])
        self.assertFalse(clean)
        self.assertIn("  - not scanned: fedcba9876543210/big.json (content is over 1,048,576 bytes)", text)

    def test_a_failed_scan_is_not_clean_even_without_incidents(self):
        text, clean = ggshield_scan.summarize([ggshield_scan.Scan("history", 128, {})])

        self.assertFalse(clean)
        self.assertIn("history: ggshield exited 128", text)
        self.assertTrue(text.rstrip().endswith("VERDICT: FINDINGS"))


class HookTest(unittest.TestCase):
    """The pre-commit hook runs ggshield only at the version runtime/ggshield-requirements.lock pins."""

    def run_hook(self, version, *args):
        with tempfile.TemporaryDirectory() as work:
            fake = Path(work, "ggshield")
            fake.write_text("#!/bin/sh\n"
                            f'if [ "$1" = --version ]; then echo "ggshield, version {version}"; exit 0; fi\n'
                            'echo "ran: $*"; exit 3\n')
            fake.chmod(0o755)
            return subprocess.run([sys.executable, str(ROOT / "scripts/ggshield_hook.py"), *args], capture_output=True,
                                  text=True, env={**os.environ, "PATH": f"{work}:/usr/bin:/bin"})

    def test_the_pinned_version_scans_the_staged_changes_or_the_pushed_commits(self):
        pinned = re.search(r"^ggshield==(\S+) ", (ROOT / "runtime/ggshield-requirements.lock").read_text(), re.M)[1]

        for mode in ("pre-commit", "pre-push"):
            done = self.run_hook(pinned, mode)

            self.assertEqual((done.returncode, done.stdout), (3, f"ran: secret scan {mode}\n"))

    def test_another_version_is_refused_with_the_reason(self):
        done = self.run_hook("0.0.1", "pre-commit")

        self.assertEqual(done.returncode, 1)
        self.assertIn("needs ggshield", done.stderr)
        self.assertIn("found: 0.0.1", done.stderr)


if __name__ == "__main__":
    unittest.main()
