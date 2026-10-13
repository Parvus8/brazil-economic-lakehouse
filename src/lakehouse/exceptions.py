class SourceUnavailableError(Exception):
    pass


class SchemaMismatchError(Exception):
    pass


class EmptyDatasetError(Exception):
    pass


class DownloadTooLargeError(Exception):
    pass


class DataQualityError(Exception):
    pass


class DownloadInterruptedError(Exception):
    pass
