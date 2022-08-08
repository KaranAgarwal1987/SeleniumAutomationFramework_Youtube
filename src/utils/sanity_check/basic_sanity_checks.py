import collections
import numbers
import re
from framework import util_functions as i6_utils

# decorators
def SanityCheckInput(input_method):
    def _wrapper(self, *args, **kwargs):
        # get_input method being executed and stored in variable
        x = input_method(self, *args,**kwargs)
        static_all = self._api.select_att(args[0], args[1], None, args[2],True,200)
        sanity_all = self._get_sanity_configs(static_all)
        sanity_all = sanity_all
        # inject sanity_all into results of get_input
        x.input_results['sanity_all'] = sanity_all
        return x
    return _wrapper

def SanityCheckValidate(calc_method):
    def _wrapper(self, *args, **kwargs):
        Input_Values = self.dict_to_tuple(args[0].input_results)
        # Pre calculation method validation
        # These will fail on first error
        self._sanity_check_error_priority(Input_Values)
        # calc method being executed and stored in variable
        x = calc_method(self, *args, **kwargs)
        # Post calculation method validation
        # These will try to find all failures
        # TODO: Maybe get more ability to put in more temporal values in Input_Values
        result = self._sanity_check_warning_priority(Input_Values)
        calc_warnings = ''
        if 'VALIDATION_WARNINGS' in x.final_results.keys():
            calc_warnings = str(x.final_results['VALIDATION_WARNINGS'])
        x.final_results['VALIDATION_WARNINGS'] = result + calc_warnings
        return x
    return _wrapper

def dict_to_tuple(dict_input):
    return i6_utils.dict_to_tuple(dict_input)

""" Helper method that looks at the sanity check calc params and find the param to be checked based on the validation_method_name """
""" Will sort the attribute_name as key in ascending order - allowing for attr_1, attr_2 for multi line inputs """
""" Can parse strings either delimited with '?' or ',' """
def get_values_of_sanity_check_attribute(Input_Values, attribute_name):
    param_list = []
    try:
        input_name_regex = re.compile('^' + attribute_name + '.*', re.IGNORECASE)
        od = collections.OrderedDict(sorted(Input_Values.sanity_all.items()))
        for k, v in od.items():
            if re.match(input_name_regex, str(k)) and v is not None and str(v).strip() != '':
                try:
                    indx = v.index('?')
                    param_list += [x.strip() for x in v.split('?')]
                except:
                    param_list += [x.strip() for x in v.split(',')]
    except:
        param_list = None
    if len(param_list) < 1:
        param_list = None

    return param_list

""" Must not be None nor empty value """
""" Tied to calculator/index/family as you need to decide what param to check """
def sc_param_exist(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_exist')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            this_attr = get_values_from_param(Input_Values, param)
            if (len(this_attr) < 1):
                this_attr = None
        except:
            pass  # don't need to do anything as the code further down will

        if this_attr is None or str(this_attr).strip() == '':
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed ' + param
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

""" Must be > than 0 checks """
""" Tied to calculator/index/family as you need to decide what param to check """
def sc_param_gt_zero(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_gt_zero')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            for this_attr in get_values_from_param(Input_Values, param):
                if float(this_attr) <= float(0):
                    raise ValueError()
        except:
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed %s (%s)' % (param, this_attr)
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

""" Must be of number """
""" Tied to calculator/index/family as you need to decide what param to check """
def sc_param_is_num(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_is_num')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            for this_attr in get_values_from_param(Input_Values, param):
                if not isinstance(this_attr, numbers.Number):
                    raise ValueError()
        except:
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed %s (%s)' % (param, this_attr)
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

""" Must be type boolean (1, 0, False, True) """
""" Tied to calculator/index/family as you need to decide what param to check """
def sc_param_is_bool(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_is_bool')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            for this_attr in get_values_from_param(Input_Values, param):
                if(str(this_attr) not in ['0','1'] and not isinstance(this_attr,bool)):
                    raise ValueError()
        except:
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed %s (%s)' % (param, this_attr)
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

""" from 0 to 1 range"""
def sc_param_is_fractional(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_is_fractional')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            for this_attr in get_values_from_param(Input_Values, param):
                if(this_attr < 0 or this_attr > 1):
                    raise ValueError()
        except:
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed %s (%s)' % (param, this_attr)
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

def sc_param_is_str(Input_Values, break_on_first_issue=False, param_list=None):
    PARAM_PREFIX = 'VALIDATION_WARNING.'
    if(break_on_first_issue):
        PARAM_PREFIX = 'VALIDATION_ERROR.'
    if param_list is None:
        try:
            param_list =  get_values_of_sanity_check_attribute(Input_Values, PARAM_PREFIX + 'sc_param_is_str')
            if param_list is None:
                param_list = []
        except:
            param_list = []

    results = dict()
    results['PASSED'] = True
    results['PARAM_RESULTS'] = []

    for param in param_list:
        this_attr = None
        try:
            for x in get_values_from_param(Input_Values, param):
                this_attr = x
                if(not isinstance(this_attr, str)):
                    raise ValueError()
        except:
            param_result = dict()
            param_result['PASSED'] = False
            param_result['MSG'] = 'Failed %s (%s)' % (param, this_attr)
            results['PARAM_RESULTS'].append(dict_to_tuple(param_result))
            results['PASSED'] = False
            if break_on_first_issue:
                break  # must stop at first sign of trouble

    return_results = dict_to_tuple(results)
    return return_results

""" Helper method to return a value, even if it is part of a list or a dictionary """
def get_values_from_param(Input_Values, param):
    value = []
    value_obj = getattr(Input_Values, param)
    if value_obj is not None:
        if type(value_obj) is dict or type(value_obj) is list:
            if type(value_obj) is list:
                value = value_obj
            else:
                value = list(value_obj.values())
        else:
            value.append(value_obj)
    return value

""" Other ideas:
** make sure a field is token separated value (like 'csv')
** make sure 
"""