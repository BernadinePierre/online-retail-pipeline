import os

def get_project_root():
    """Get the absolute path to the project root"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_data_path(*subpaths):
    """Get absolute path to data directory"""
    return os.path.join(get_project_root(), 'data', *subpaths)

def get_logs_path():
    """Get absolute path to logs directory"""
    return get_data_path('logs')