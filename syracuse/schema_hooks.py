def filter_paths(endpoints):
    """Filter schema to include only endpoints under /api/v1/activities/"""
    return [
        (path, path_regex, method, callback)
        for (path, path_regex, method, callback) in endpoints
        if path.startswith('/api/v1/')
    ]