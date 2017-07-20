# -*- coding: utf-8 -*-

def get_parameter(args, match):
    """
    从启动项中获得参数,参数格式为 test.py d/2008-01-11 t/19:23:58
    get_parameter(sys.argv,'d')
    """
    for arg in args:
        if arg.find(match + '/') == 0:
            l = len(match)
            return arg[l + 1:]
    return None


def check_para(args, match, default):
    """
    检查参数信息
    """
    para = get_parameter(args, match)
    if para is None:
        para = default
    return para
