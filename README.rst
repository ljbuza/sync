##handle_undefined_codes(tc):

    Inserts all undefined codes found in fact table into dimension table, using
    the 'code_source' table for mapping.

    Logs entries into the 'undefined_log' table.

    handle_undefined_codes does the lookup in the code_source table, starts a
    queue for multipprocessing runs of the 'get_undefined_codes' function.

    :param tc: MyClient instance
    :returns: None
