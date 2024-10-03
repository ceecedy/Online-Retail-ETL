# include/soda/check_function.py
def check(scan_name, checks_subpath=None, data_source='retail', project_root='include'):
    # import the scan class
    from soda.scan import Scan

    print('Running Soda Scan ...')
    # assign variables for the config path and check function path
    config_file = f'{project_root}/soda/configuration.yml'
    checks_path = f'{project_root}/soda/checks'

    # if checks subpath is not none, then there would be a function loaded to test. 
    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    # assigning essential values to soda before executing the test. 
    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan failed')

    return result