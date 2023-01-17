import json
import datetime 
import os.path as path

class Common():
    '''Common values and constants used over the project'''

    #app.config file source
    APP_CONFIG_FILE = 'conf/app.config.json'
    Configuration = dict()
            
    def read_appconfig() -> object:
        ''' read the configuration file '''
        try:
            proj_path = path.abspath(__file__)
            dir_business = path.dirname(proj_path)
            dir_src = path.dirname(dir_business)
            file_path = path.join(dir_src, Common.APP_CONFIG_FILE)
            appconf = dict()
            with (open(file_path, mode='r', encoding='utf-8')) as f:                
                appconf = json.loads(f.read())
            return appconf['Configuration']
        except Exception as ex:
            return print('App.config there is not exists.')