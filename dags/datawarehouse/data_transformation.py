from datetime import timedelta, datetime


def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")

    componets = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for i in componets:
        if i in duration_str:
            value, duration_str = duration_str.split(i)
            values[i] = int(value)

    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )

    return total_duration


def check_quality(definition):
    if "hd" in definition.lower():
        is_hd = True
    else:
        is_hd = False
    return is_hd


def count_tags(tags):
    sum = 0
    for i in tags:
        sum += 1
    return sum


def extract_categories(categories):
    return [i.split("wiki/")[1] for i in categories]


def transform_data(row):

    duration_td = parse_duration(row["Duration"])

    row["Duration"] = (datetime.min + duration_td).time()

    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    row["Is_hd"] = check_quality(row["Definition"])

    row["Tag_count"] = count_tags(row["Tags"])

    row["Topic_Categories"] = extract_categories(row["Topic_Categories"])

    return row
