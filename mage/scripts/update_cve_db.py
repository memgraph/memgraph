from cve_bin_tool.cvedb import CVEDB


def main():
    """
    Updates the CVE database for cve-bin-tool prior to scanning.
    """
    print("Updating CVE Database (this can take ages!)")
    cve_db = CVEDB()
    cve_db.refresh_cache_and_update_db()
    print("Done!!!")

    # cve_db does unusual things when it exists, so let's catch it
    try:
        del cve_db
    except Exception:
        pass


if __name__ == "__main__":
    main()
