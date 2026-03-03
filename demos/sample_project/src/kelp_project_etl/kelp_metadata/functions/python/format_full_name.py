def format_full_name(first_name: str | None, last_name: str | None) -> str | None:
    if first_name and last_name:
        return f"{first_name.strip().title()} {last_name.strip().title()}"
    if first_name:
        return first_name.strip().title()
    if last_name:
        return last_name.strip().title()
    return None


return format_full_name(first_name, last_name)
