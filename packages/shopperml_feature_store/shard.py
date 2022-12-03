import datetime
import hashlib

TOTAL_SHARDS = 20
SHARD_PADDING = 2
FORMAT_SWITCHOVER_DATE = datetime.date(2020, 11, 19)


def format_key_prefix(date):
    return "{:04d}{:02d}{:02d}".format(date.year, date.month, date.day)


def hash_shopper_id(shopper_id, modulus):
    """
    Given a shopper_id (as string), return a md5 hash of the shopper_id mod a given modulus
        as an int between 0 and modulus - 1
    This function is required to be:
        1. deterministic
        2. consistent across time (will be used to determine what prefix or shard a given shopper is in),
        3. consistent between sessions (unlike the default hash) and
        4. have roughly equal probability of putting a shopper in any prefix or shard
    To meet these criteria, we perform the following steps:
        1. Encode the shopper_id into utf-8
        2. Hash that encoding with md5 into a hex string
        3. Convert that hex string to an integer
        4. Mod that integer with the number of total prefixes or shards
    """
    byte_string = shopper_id.encode("utf-8")
    hashed_hexvalue = hashlib.md5(byte_string).hexdigest()
    integer_value = int(hashed_hexvalue, 16)
    return integer_value % modulus


def get_shard_prefix(shopper_id):
    """
    Given a shopper_id (as string) and number of total_shards,
        return a zero-padding string representation of a value
        between 0 and total_shards - 1
    """
    prefix_value = hash_shopper_id(shopper_id, TOTAL_SHARDS)
    return "{0:0{pad}d}".format(prefix_value, pad=SHARD_PADDING)


def standardize_shopper_id(shopper_id):
    """
    Verifies that the shopper_id fits certain standards (erroring if not):
        1. Shopper ID is a string
        2. Length of shopper ID is more than 0
        2. Shopper ID contains only alpha-numeric characters
    Forces shopper_id to conform to other standards:
        1. All lowercase
    """
    assert isinstance(shopper_id, str)
    assert len(shopper_id) > 0
    assert shopper_id.isalnum()
    return shopper_id.lower()


def get_s3_shopper_path(shopper_id, date):
    """
    Given shopper_id and date, returns the s3 path to that shopper at that date
    """
    date_string = format_key_prefix(date)
    if date > FORMAT_SWITCHOVER_DATE:
        standardized_shopper_id = standardize_shopper_id(shopper_id)
        shard_prefix = get_shard_prefix(standardized_shopper_id)
        return "{}/{}/{}".format(shard_prefix, date_string, standardized_shopper_id)
    else:
        return "{}/{}".format(date_string, shopper_id)
