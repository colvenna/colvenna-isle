"""Consumer fault probes find the archived original even where a missing key answers 403."""

import unittest

from botocore.exceptions import ClientError

from colvenna_isle.writer.consumer import archived_ingestion

SPACE, GENERATION = "probe_consumer_space", "gen_" + "a" * 32
INGESTION = "ing_" + "b" * 64


class S3:
    """Without s3:ListBucket on ingest/, HEAD of an absent key answers 403, not 404."""

    class exceptions:
        ClientError = ClientError

    def __init__(self, metadata, missing="403"):
        self.metadata, self.missing, self.keys = metadata, missing, []

    def head_object(self, Bucket, Key, ExpectedBucketOwner):
        self.keys.append(Key)
        if Key.endswith("/orders/" + INGESTION + ".json"):
            return {"Metadata": self.metadata}
        raise ClientError({"Error": {"Code": self.missing, "Message": "Forbidden"}}, "HeadObject")


IDENTITY = {"space-id": SPACE, "space-generation": GENERATION, "ingestion-id": INGESTION}
DATASETS = ["ads.account_totals", "dwd.accounts", "ods.orders", "ods.products"]


class ConsumerProbe(unittest.TestCase):
    def test_absent_keys_answering_403_or_404_are_skipped(self):
        for missing in ["403", "404", "NoSuchKey"]:
            s3 = S3(IDENTITY, missing)
            self.assertTrue(archived_ingestion(s3, "bucket", "acct", SPACE, GENERATION, DATASETS, INGESTION))
            self.assertEqual(s3.keys[-1], f"spaces/{SPACE}/ingest/{GENERATION}/orders/{INGESTION}.json")

    def test_identity_must_match_the_archive_metadata(self):
        for field in IDENTITY:
            self.assertFalse(archived_ingestion(S3({**IDENTITY, field: "other"}), "bucket", "acct", SPACE, GENERATION,
                                                DATASETS, INGESTION))
        self.assertFalse(archived_ingestion(S3(IDENTITY), "bucket", "acct", SPACE, GENERATION, ["dwd.accounts"],
                                            INGESTION))

    def test_other_errors_are_not_hidden(self):
        with self.assertRaises(ClientError):
            archived_ingestion(S3(IDENTITY, "SlowDown"), "bucket", "acct", SPACE, GENERATION, DATASETS, INGESTION)


if __name__ == "__main__":
    unittest.main()
