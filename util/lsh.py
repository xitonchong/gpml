import os 
import json 
import numpy as np 
from scipy import sparse
from scipy.sparse import csr_matrix 


from .lsh_storage import storage, serialize, deserialize


class LSH(object): 
    '''LSH implements locality sensitive hashing usi0ng random projection for
        input vectors of dimension `input_dim`. 

        Attributes: 
        :param hash_size: 
            The length of the rsulting binary hash in integer. E.g., 32 means 
            the resulting binary hahs will be 32-bit long. 
        :param input_dim: 
            The dimension of the input vector. This can be found in your sparse 
            matrix by checking the .shape attribute of your matrix. I.E., 
            `csr_dataset.shape[1]`. 
        :param num_hashtables: 
            (optional) the number of hash tables used for multiple look-ups. 
            increasing teh number of hashtables increases the probabiliyt of 
            a hash collision of similar documents, but it also increases the 
            amount of work needed to add points. 
        :param storage config: 
            (optional) A dictionar of the form `{backend_name: config}` where
            `backend_name` is either `dict`, `berkekeydb`, `leveldb` or
            `redis`. `config` is the configuration used by backend . 
            Example configs for each type are as follows: 
            `In-memory Python Dictionary`:
                {"dict": None} # takes no options 
            `Redis`: 
                `{"redis": {"host": hostname, "port": port_num}}`. 
                Where `hostname` is normally `localhost` and `port` is normally 6379.
            `LevelDB`:
                {'leveldb':{'db': 'ldb'}}
                Where 'db' specifies the directory to store the LevelDB database.
            `Berkeley DB`:
                {'berkeleydb':{'filename': './db'}}
                Where 'filename' is the location of the database file.
            NOTE: Both Redis and Dict are in-memory. Keep this in mind when
            selecting a storage backend.
        :param matrices_filename:
            (optional) Specify the path to the compressed numpy file ending with
            extension `.npz`, where the uniform random planes are stored, or to be
            stored if the file does not exist yet.
        :param overwrite:
            (optional) Whether to overwrite the matrices file if it already exist.
            This needs to be True if the input dimensions or number of hashtables
            change.
    '''
    
    def __init__(self,hash_size,input_dim, num_hashtables=1, 
                 storage_config=None, matrices_filename=None, overwrite=False):
        self.hash_size = hash_size 
        self.input_dim = input_dim 
        self.num_hashtables = num_hashtables

        if storage_config is None: 
            storage_config = {'dict':None}
        self.storage_config = storage_config 

        if matrices_filename and not matrices_filename.endswith('.npz'):
            raise ValueError("the specified file name must end with .npz")  
        self.matrices_filename = matrices_filename 
        self.overwrite = overwrite 

        self._init_uniform_planes() 
        self._init_hashtables() 


    def _init_uniform_planes(self): 

        if "uniform_planes" in self.__dict__:
            return 
        
        if self.matrices_filename: 
            file_exist = os.path.isfile(self.matrices_filename)
            if file_exist and not self.overwrite: 
                try: 
                    # TODO: load sparse file
                    npzfiles = np.load(self.matrices_filename) 
                except IOError: 
                    print("cannot load specified file as numpy array")
                    raise 
                else: 
                    npzfiles = sorted(list(npzfiles.items()), key=lambda x: x[0])
                    # TODO: to sparse
                    self._init_uniform_planes = [t[1] for t in npzfiles]

                


