from airflow.contrib.hooks.sftp_hook import SFTPHook

def check_file(**context):
    """Recieves as input the path to a file, a connection ID for the target host with
    the file and tests if the file exists, returning the defined values from inputs
    if_true and if_false

    Inputs:
        templates_dict:
            f_path str -- full path to file (templated)
        op_kwargs:
            if_true [any] -- returned value if file exists
            if_false [any] -- returned value if file doesn't exists

    Returns:
        [any] -- return value defined by if_true and if_false
    """
    f_path = context['templates_dict']['file_path']
    conn = context['conn_id']
    if_true = context['id_true']
    if_false = context['id_false']

    sh = SFTPHook(conn)

    if sh.path_exists(f_path):
        return if_true
    else:
        return if_false


def evaluate_var(**context):
    """Receives 2 variables, the evaluation condition and the 2 results for the case of 
    result is True or False 

    Inputs:
        templates_dict:
            var_in str -- value 1 to evaluate
            var_eval str -- value 2 of the condition to evaluate
        op_kwargs:
            expr str -- must be <,>,=,!=,<=,>= represents the evaluation condition
            if_true [any] -- return value if result is True
            if_false [any] -- return value if result is False

    Returns:
        [any] -- return value defined by if_true and if_false
    """

    def eval_expr(if_statement, iftrue, iffalse):
        if if_statement:
            return iftrue
        else:
            return iffalse

    var_in = context['templates_dict']['var_in']
    var_eval = context['templates_dict']['var_eval']
    expression = context['expr']

    if_true = context['id_true']
    if_false = context['id_false']

    if expression == '=':
        return eval_expr(var_in == var_eval, if_true, if_false)
    elif expression == '>=':
        return eval_expr(var_in >= var_eval, if_true, if_false)
    elif expression == '>':
        return eval_expr(var_in > var_eval, if_true, if_false)
    elif expression == '<=':
        return eval_expr(var_in <= var_eval, if_true, if_false)
    elif expression == '<':
        return eval_expr(var_in < var_eval, if_true, if_false)
    elif expression == '!=':
        return eval_expr(var_in != var_eval, if_true, if_false)