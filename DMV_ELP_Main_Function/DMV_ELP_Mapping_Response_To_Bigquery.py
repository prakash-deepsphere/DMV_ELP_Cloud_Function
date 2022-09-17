def Process_API_Response(vAR_api_response):    
   vAR_data = {}
   
   vAR_data['ERROR_MESSAGE'] = vAR_api_response['Error Message']

   if len(vAR_api_response['Error Message'])==0:
      vAR_data['REQUEST_ID'] = vAR_api_response['Request Id']
      vAR_data['REQUEST_DATE'] = vAR_api_response['Request Date']
      vAR_data['ORDER_DATE'] = vAR_api_response['Order Date']
      vAR_data['CONFIGURATION'] = vAR_api_response['Configuration']
      vAR_data['ORDER_ID'] = vAR_api_response['Order Id']
      vAR_data['SG_ID'] = vAR_api_response['Simply Gov Id']
      vAR_data['ORDER_GROUP_ID'] = vAR_api_response['Order Group Id']

      vAR_data['PREVIOUSLY_DENIED'] = vAR_api_response['Previously Denied']
      if vAR_api_response['Direct Profanity']['Is accepted']:
         vAR_data['DIRECT_PROFANITY'] = 'APPROVED - Not falls under any of the profanity word'
      if not vAR_api_response['Direct Profanity']['Is accepted']:
         vAR_data['DIRECT_PROFANITY'] = 'DENIED - '+vAR_api_response['Direct Profanity']['Message']

      vAR_data['GUIDELINE_FWORD'] = vAR_api_response['FWord Guideline Validation']
      
      if vAR_api_response['Denied Pattern']['Is accepted']:
         vAR_data['RULE_BASED_CLASSIFICATION'] = 'APPROVED - Not falls under any of the denied patterns'
      if not vAR_api_response['Denied Pattern']['Is accepted']:
         vAR_data['RULE_BASED_CLASSIFICATION'] = 'DENIED - '+vAR_api_response['Denied Pattern']['Message']

      if 'Model Prediction'  in vAR_api_response:
         vAR_data['MODEL'] = 'RNN'
         vAR_data['TOXIC'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Toxic']
         vAR_data['SEVERE_TOXIC'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Severe Toxic']
         vAR_data['OBSCENE'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Obscene']
         vAR_data['IDENTITY_HATE'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Identity Hate']
         vAR_data['INSULT'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Insult']
         vAR_data['THREAT'] = vAR_api_response['Model Prediction']['Profanity Classification'][0]['Threat']
         vAR_data['OVERALL_SCORE'] = vAR_api_response['Model Prediction']['Sum of all Categories']
         vAR_data['RECOMMENDATION'] = vAR_api_response['Model Prediction']['Recommendation']
         vAR_data['REASON'] = vAR_api_response['Model Prediction']['Reason']
      else:
         vAR_data['RECOMMENDATION'] = vAR_api_response['Recommendation']
         vAR_data['REASON'] = vAR_api_response['Reason']
   else:
      vAR_data['REQUEST_ID'] = vAR_api_response['Request Id']
      vAR_data['REQUEST_DATE'] = vAR_api_response['Request Date']
      vAR_data['ORDER_DATE'] = vAR_api_response['Order Date']
      vAR_data['CONFIGURATION'] = vAR_api_response['Configuration']
      vAR_data['ORDER_ID'] = vAR_api_response['Order Id']
      vAR_data['SG_ID'] = vAR_api_response['Simply Gov Id']
      vAR_data['ORDER_GROUP_ID'] = vAR_api_response['Order Group Id']
   return vAR_data 