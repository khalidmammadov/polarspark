from urllib.parse import urlparse, urlunparse
from pathlib import Path


def resolve_uri(path: str):
    try:
        parsed_uri = urlparse(path)
        if parsed_uri.scheme:
            return parsed_uri

        # Handle paths with fragments
        if parsed_uri.fragment:
            absolute_path = Path(parsed_uri.path).resolve()
            absolute_uri = absolute_path.as_uri()
            # Recreate URI with the fragment
            scheme, netloc, path, params, query, fragment = urlparse(absolute_uri)
            return urlunparse((scheme, netloc, path, params, query, parsed_uri.fragment))
    except Exception:
        pass

    return Path(path).resolve().as_uri()
