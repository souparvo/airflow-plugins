from airflow.contrib.hooks.sftp_hook import SFTPHook


def check_file(**context):
    """Recebe como input path para um ficheiro, uma conn id para a maquina com o ficheiro e 
    testa se o ficheiro existe retornando os valore definidos nos inputs if_true e if_false

    Inputs:
        templates_dict:
            f_path str -- full path para o ficheiro (templated)
        op_kwargs:
            if_true [any] -- valor de retorno se ficheiro existir
            if_false [any] -- valor de retorno se ficheiro não existir

    Returns:
        [any] -- Retorno definido pelas variaveis if_true e if_false
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
    """Recebe 2 variaveis, uma condicao avaliadora e 2 resultados para caso se ja True ou False
    o resultado do expressao avaliada.

    Inputs:
        templates_dict:
            var_in str -- valor 1 para avaliar
            var_eval str -- valor 2 da condicao para avaliar
        op_kwargs:
            expr str -- deve ser <,>,=,!=,<=,>= representa a expressão avaliadora
            if_true [any] -- valor de retorno se ficheiro existir
            if_false [any] -- valor de retorno se ficheiro não existir

    Returns:
        [any] -- Retorno definido pelas variaveis if_true e if_false
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