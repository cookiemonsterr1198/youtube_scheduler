from dateutil import tz

# Additional Functions
def convert_timezone(prop):
    from_zone = tz.gettz("UTC")
    to_zone = tz.gettz("Asia/Jakarta")
    timestamp_utc = prop.replace(tzinfo=from_zone)
    timestamp = timestamp_utc.astimezone(to_zone)
    return timestamp


def rmv_milisec(prop):
    return (
        prop.split(".")[0] + "Z" if "." in prop else prop
    )  # hapus titik jika ada koma (.) sebelum huruf "Z"
