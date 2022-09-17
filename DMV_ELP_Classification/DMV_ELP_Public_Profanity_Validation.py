import pandas as pd

def Profanity_Words_Check(vAR_val):
    vAR_input = vAR_val
    vAR_badwords_df = pd.read_csv('gs://dmv_elp_project/data/badwords_list.csv',header=None)
    print('data - ',vAR_badwords_df.head(20))
    vAR_result_message = ""
    
#---------------Profanity logic implementation with O(log n) time complexity-------------------
    # Direct profanity check
    vAR_badwords_df[1] = vAR_badwords_df[1].str.upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df[1],vAR_input)
    if vAR_is_input_in_profanity_list!=-1:
        vAR_result_message = 'Input ' +vAR_val+ ' matches with direct profanity - '+vAR_badwords_df[1][vAR_is_input_in_profanity_list]
        
        return True,vAR_result_message
    
    # Reversal profanity check
    vAR_reverse_input = "".join(reversed(vAR_val)).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df[1],vAR_reverse_input)
    if vAR_is_input_in_profanity_list!=-1:
        vAR_result_message = 'Input ' +vAR_val+ ' matches with reversal profanity - '+vAR_badwords_df[1][vAR_is_input_in_profanity_list]
        return True,vAR_result_message
    
    # Number replacement profanity check
    vAR_number_replaced = Number_Replacement(vAR_val).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df[1],vAR_number_replaced)
    if vAR_is_input_in_profanity_list!=-1: 
       vAR_result_message = 'Input ' +vAR_val+ ' matches with number replacement profanity - '+vAR_badwords_df[1][vAR_is_input_in_profanity_list]
       return True,vAR_result_message
    
    # Reversal Number replacement profanity check(5sa->as5->ass)
    vAR_number_replaced = Number_Replacement(vAR_reverse_input).upper()
    vAR_is_input_in_profanity_list = Binary_Search(vAR_badwords_df[1],vAR_number_replaced)
    if vAR_is_input_in_profanity_list!=-1:  
        vAR_result_message = 'Input ' +vAR_val+ ' matches with reversal number replacement profanity - '+vAR_badwords_df[1][vAR_is_input_in_profanity_list]
        return True,vAR_result_message
    
    print('1st lvl message - ',vAR_result_message)
    return False,vAR_result_message


def Number_Replacement(vAR_val):
    vAR_output = vAR_val
    if "1" in vAR_val:
        vAR_output = vAR_output.replace("1","I")
    if "2" in vAR_val:
        vAR_output = vAR_output.replace("2","Z")
    if "3" in vAR_val:
        vAR_output = vAR_output.replace("3","E")
    if "4" in vAR_val:
        vAR_output = vAR_output.replace("4","A")
    if "5" in vAR_val:
        vAR_output = vAR_output.replace("5","S")
    if "8" in vAR_val:
        vAR_output = vAR_output.replace("8","B")
        print('8 replaced with B - ',vAR_val)
    if "0" in vAR_val:
        vAR_output = vAR_output.replace("0","O")
    print('number replace - ',vAR_output)
    return vAR_output



def Binary_Search(data, x):
    low = 0
    high = len(data) - 1
    mid = 0
    i =0
    while low <= high:
        i = i+1
        print('No.of iteration - ',i)
        mid = (high + low) // 2
        
        # If x is greater, ignore left half
        if data[mid] < x:
            low = mid + 1
 
        # If x is smaller, ignore right half
        elif data[mid] > x:
            high = mid - 1
 
        # means x is present at mid
        else:
            return mid
 
    # If we reach here, then the element was not present
    return -1

