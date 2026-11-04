"""Seams of the publication secret scan (scripts/secret_scan.py) that run without ggshield or the network."""

import io
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import secret_scan  # noqa: E402


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

            result = secret_scan.unpack([first, second], out)

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

            secret_scan.unpack([source], out)

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

            result = secret_scan.unpack([build], out)

            self.assertEqual(stored_files(out), [b"VERSION = '0'\n", b"checks passed\n"])
            self.assertEqual(result.files, 2)


class ForcePushedHeadsTest(unittest.TestCase):
    def test_lists_each_replaced_head_once_across_pages(self):
        def page(nodes):
            return {"data": {"repository": {"pullRequests": {"nodes": nodes}}}}

        pages = [
            page([{"number": 3, "timelineItems": {"nodes": [
                {"beforeCommit": {"oid": "a" * 40}, "afterCommit": {"oid": "b" * 40}},
                {"beforeCommit": None, "afterCommit": {"oid": "c" * 40}},
            ]}}, {"number": 4, "timelineItems": {"nodes": []}}]),
            page([{"number": 9, "timelineItems": {"nodes": [
                {"beforeCommit": {"oid": "d" * 40}, "afterCommit": {"oid": "e" * 40}},
                {"beforeCommit": {"oid": "a" * 40}, "afterCommit": {"oid": "f" * 40}},
            ]}}]),
        ]

        self.assertEqual(secret_scan.force_pushed_heads(pages), [(3, "a" * 40), (9, "d" * 40)])


SECRET = "s3cr3t-Value-0123456789abcdef"


def incident(detector, ignore_sha, line):
    return {"type": detector, "ignore_sha": ignore_sha, "validity": "no_checker", "total_occurrences": 1,
            "occurrences": [{"match": SECRET, "type": "apikey", "line_start": line, "line_end": line,
                             "index_start": 10, "index_end": 39}]}


class SummarizeTest(unittest.TestCase):
    def test_no_incident_in_any_scope_is_clean(self):
        scans = [
            secret_scan.Scan("history", 0, {"type": "commit-range", "total_incidents": 0, "scans": []}),
            secret_scan.Scan("release assets", 0, {"type": "path_scan", "total_incidents": 0,
                                                   "entities_with_incidents": []}),
        ]

        text, clean = secret_scan.summarize(scans)

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

        text, clean = secret_scan.summarize([secret_scan.Scan("history", 1, history),
                                             secret_scan.Scan("artifacts", 1, artifacts)])

        self.assertFalse(clean)
        self.assertNotIn(SECRET, text)
        self.assertIn("history: 1 incidents", text)
        self.assertIn("Generic High Entropy Secret, commit cccccccccccc, src/settings.py:12, ignore_sha " + "1" * 64,
                      text)
        self.assertIn("Generic Password, 0123456789abcdef/build.log:3, ignore_sha " + "2" * 64, text)
        self.assertTrue(text.rstrip().endswith("VERDICT: FINDINGS"))

    def test_a_failed_scan_is_not_clean_even_without_incidents(self):
        text, clean = secret_scan.summarize([secret_scan.Scan("history", 128, {})])

        self.assertFalse(clean)
        self.assertIn("history: ggshield exited 128", text)
        self.assertTrue(text.rstrip().endswith("VERDICT: FINDINGS"))


if __name__ == "__main__":
    unittest.main()
