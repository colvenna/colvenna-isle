"""Positive observation of an exact immutable S3 version's absence."""

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.control.registry import ControlError


def object_versions(s3, bucket, account, key, error_code):
    # GetObject on a missing version can return AccessDenied under narrow
    # bucket-list permissions. Only an authorized, complete version listing
    # establishes absence; a failed request never releases retained custody.
    try:
        if not isinstance(key, str) or not key:
            raise ValueError("Original key required")
        markers, seen = {}, set()
        versions = []
        for _ in range(32):
            response = s3.list_object_versions(Bucket=bucket, Prefix=key,
                ExpectedBucketOwner=account, MaxKeys=1000, **markers)
            for name in ["Versions", "DeleteMarkers"]:
                entries = response.get(name, [])
                if not isinstance(entries, list):
                    raise ValueError("Version observation incomplete")
                for entry in entries:
                    if not isinstance(entry["Key"], str) or not isinstance(entry["VersionId"], str):
                        raise ValueError("Version identity incomplete")
                    if entry["Key"] == key:
                        versions.append((entry["VersionId"], name == "DeleteMarkers"))
            if response["IsTruncated"] is False:
                return versions
            if response["IsTruncated"] is not True:
                raise ValueError("Version observation incomplete")
            cursor = (response["NextKeyMarker"], response["NextVersionIdMarker"])
            if any(not isinstance(value, str) or not value for value in cursor) or cursor in seen:
                raise ValueError("Version observation did not advance")
            seen.add(cursor)
            markers = dict(zip(["KeyMarker", "VersionIdMarker"], cursor))
    except (ClientError, BotoCoreError, KeyError, TypeError, ValueError):
        pass
    raise ControlError(503, error_code) from None


def version_absent(s3, bucket, account, reference, error_code):
    if (reference.get("bucket") != bucket or not isinstance(reference.get("version_id"), str)
            or reference["version_id"] in {"", "null"}):
        raise ControlError(503, error_code)
    return all(version != reference["version_id"] for version, _ in
               object_versions(s3, bucket, account, reference["key"], error_code))
