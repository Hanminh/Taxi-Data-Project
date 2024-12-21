 df = pd.DataFrame(batch)
                        write_to_hdfs(df, f'{HDFS_PATH}tripdata_{time.time()}.csv')
                        print(f'Batch of {len(batch)} records written to HDFS')
                        return