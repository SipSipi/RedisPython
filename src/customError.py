def customError(errNum):
    
    dictListError = {   'err1': {
                            'error': '1',
                            'error message': 'ERR_0001 - Unprocessable Entity!Please insert the correct claim uuid',

                        },
                        'err2': {
                            'error': '1',
                            'error message': 'ERR_0002 - Unprocessable Entity!Please insert the correct claim uuid',

                        },
                        'err3': {
                            'error': '1',
                            'error message': 'ERR_0003 - Unprocessable Entity!Please insert the correct claim uuid',

                        },
                        'err4': {
                            'error': '1',
                            'error message': 'ERR_0004 - draft is saved!',

                        },
                        'err5': {
                            'error': '1',
                            'error message': 'ERR_0005 - Unprocessable Entity!Please insert the correct claim uuid',

                        },
                        'err6': {
                            'error': '1',
                            'error message': 'ERR_0006 - Unprocessable Entity!Please insert the correct claim uuid',

                        },
    }
    return dictListError[errNum]

def customError2(errNum,msg):
    
    dictListError = {   'err1': {
                            'error': '1',
                            'error message': 'ERR_0001 - {}'.format(msg),

                        },
                        'err2': {
                            'error': '1',
                            'error message': 'ERR_0002 - {}'.format(msg),

                        }
    }
    return dictListError[errNum]