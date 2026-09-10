#!/usr/bin/env python3
"""Regression coverage for successful uploads without a retained Cargo tarball."""

import hashlib
import json
import tempfile
import types
import unittest
from pathlib import Path

import release_crates as rc


class FakeCargo:
    """cargo publish that exits 0 and leaves no tarball anywhere."""

    returncode = 0
    stdout = ""
    stderr = ""


class PublishConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output = Path(self.tmp.name)
        self.version = "0.5.0"
        self.entries = []
        self.candidate_cksums = {}
        for name in rc.CRATE_ORDER:
            data = f"verified-candidate-bytes:{name}:{self.version}".encode()
            file_name = f"{name}-{self.version}.crate"
            (self.output / file_name).write_bytes(data)
            cksum = hashlib.sha256(data).hexdigest()
            self.candidate_cksums[name] = cksum
            self.entries.append({"name": name, "version": self.version,
                                 "file": file_name, "sha256": cksum,
                                 "size": len(data)})
        (self.output / "SHA256SUMS").write_text(
            "".join(f"{e['sha256']}  {e['file']}\n" for e in self.entries),
            encoding="utf-8")
        self.sums_before = (self.output / "SHA256SUMS").read_bytes()

        self.uploads = []
        self.registry_cksums = {}  # name -> checksum the index reports

        def fake_run(argv, **kwargs):
            self.uploads.append(list(argv))
            return FakeCargo()

        def fake_lookup(name, version):
            return "absent", None

        def fake_wait(name, version, timeout_s):
            return "visible", self.registry_cksums[name]

        self._saved = (rc.subprocess, rc.index_lookup,
                       rc.wait_for_index_cksum, rc.guard_clean_unchanged)
        rc.subprocess = types.SimpleNamespace(run=fake_run)
        rc.index_lookup = fake_lookup
        rc.wait_for_index_cksum = fake_wait
        rc.guard_clean_unchanged = lambda root, sha: None

    def tearDown(self):
        (rc.subprocess, rc.index_lookup, rc.wait_for_index_cksum,
         rc.guard_clean_unchanged) = self._saved
        self.tmp.cleanup()

    def manifest(self):
        return json.loads((self.output / "release-manifest.json")
                          .read_text(encoding="utf-8"))

    def published_names(self):
        return [argv[argv.index("-p") + 1] for argv in self.uploads]

    def test_success_without_local_tarball_confirms_all_four(self):
        # Cargo exits 0 and creates nothing; the index reports exactly each
        # candidate's checksum.
        self.registry_cksums.update(self.candidate_cksums)

        rc.publish_crates(Path("/nonexistent-root"), self.version, "0" * 40,
                          self.output, self.entries)

        manifest = self.manifest()
        self.assertEqual(
            [r["status"] for r in manifest["publish"]["results"]],
            ["published"] * len(rc.CRATE_ORDER))
        self.assertEqual(
            manifest["publish"]["retention"],
            {name: "retained" for name in rc.CRATE_ORDER})
        for name in rc.CRATE_ORDER:
            # The receipt is bound to the verified candidate bytes, which
            # remain in the output directory untouched.
            data = (self.output / f"{name}-{self.version}.crate").read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(),
                             self.candidate_cksums[name])
        # Uploads are ordered and retained checksums still describe the archives.
        self.assertEqual(self.published_names(), rc.CRATE_ORDER)
        self.assertEqual((self.output / "SHA256SUMS").read_bytes(),
                         self.sums_before)

    def test_registry_checksum_mismatch_stays_unconfirmed_and_stops(self):
        self.registry_cksums[rc.CRATE_ORDER[0]] = "de" * 32
        for name in rc.CRATE_ORDER[1:]:
            self.registry_cksums[name] = self.candidate_cksums[name]

        with self.assertRaises(SystemExit):
            rc.publish_crates(Path("/nonexistent-root"), self.version,
                              "0" * 40, self.output, self.entries)

        manifest = self.manifest()
        statuses = {r["name"]: r["status"]
                    for r in manifest["publish"]["results"]}
        retention = manifest["publish"]["retention"]
        self.assertEqual(statuses[rc.CRATE_ORDER[0]], "unconfirmed")
        self.assertEqual(retention[rc.CRATE_ORDER[0]], "untouched")
        for name in rc.CRATE_ORDER[1:]:
            self.assertEqual(statuses[name], "not-attempted")
            self.assertEqual(retention[name], "untouched")
        # The mismatch prevented every later upload.
        self.assertEqual(self.published_names(), [rc.CRATE_ORDER[0]])


if __name__ == "__main__":
    unittest.main()
