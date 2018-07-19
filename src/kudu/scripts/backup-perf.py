#!/usr/bin/python
#################################################################
# Backup and Restore performance test driver.
#################################################################

def create_table(table_options):
    """ Create a Kudu table via impala-shell """
    table_name = ""
    return table_name
    pass

def load_table(table_name, load_options):
    # TODO: Invoke spark table load job.
    pass

def backup_table(table_name, backup_options):
    # TODO: Invoke spark backup job.
    pass

def restore_table(table_name, backup_options):
    restored_table_name = table_name + ".restored"
    # TODO: Invoke spark restore job.
    return restored_table_name

def main(args):
    # TODO: Parse command-line arguments.

    table_name = create_table(table_options)

    load_table(table_name, load_options)

    backup_table(table_name, backup_options)

    restored_table_name = restore_table(table_name, restore_options)

if __name__ == "__main__":
    main(sys.args)
    sys.exit(0)
