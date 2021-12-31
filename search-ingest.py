def funcx_func(event):
    from globus_sdk import SearchClient
    import globus_sdk

    search_token = event['search_token']
    mdata_dir = event['mdata_dir']
    dataset_mdata  = event['dataset_mdata']

    # Auth with search
    sc = globus_sdk.SearchClient(authorizer=globus_sdk.authorizers.AccessTokenAuthorizer(access_token=search_token))
                                    
    # TODO: hardcode.
    files_to_ingest = '/home/tskluzac/mdata'

    # TODO: generate feedstock
    # # TODO: ingest.
    return "HELLO WORLD"

funcx_func('hi')
