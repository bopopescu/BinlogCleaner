import json
import logging
import traceback

logger = logging.getLogger("cleaner")

def return_value(**kwargs):
    global logger
    try:
        result = {}
        if 'status' in kwargs:
            result['status'] = kwargs['status']
        else:
            result['status'] = -1
        
        if result['status'] == 0:
            if 'val' in kwargs:
                result['val'] = kwargs['val']
            else:
                result['val'] = None
        elif result['status'] == -1:
            result['err'] = {}
            if 'event' in kwargs:
                result['err']['event'] = kwargs['event']
            else:
                result['err']['event'] = None
            if 'msg' in kwargs:
                result['err']['msg'] = kwargs['msg']
            else:
                result['err']['msg'] = None
    
        if not ('json' in kwargs):
            kwargs['json'] = 1
        
        if kwargs['json'] == 1:
            return json.dumps(result)
            
        else:
            result_str = None
            if result['status'] == 0:
                result_str = '''{"status": "%d", "val": "''' %(result['status']) + result['val'] + '''"}'''
            elif result['status'] == -1:
                result_str = '''{"status": "%d", "err": {"msg": "%s", "event": "%s"}}''' %(result['status'], result['err']['msg'], result['err']['event'])
            return result_str

    except:
        msg = traceback.format_exc()
        logger.error(msg)
        return '''{"status": -1, "err": {"msg": "return_value error", "event": "LIBS.RETVAL"}}'''
