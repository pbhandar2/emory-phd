mkdir ~/disk 
sudo mkfs -t ext4 /dev/sdb
sudo mount /dev/sdb ~/disk
sudo chown vishwa ~/disk

sudo mkdir /research2
sudo mkfs -t ext4 /dev/sdc 
sudo mount /dev/sdc /research2
sudo chown vishwa /research2

sudo apt-get update 
sudo apt install -y python3-pip clang 
sudo chown vishwa ~/disk

git clone https://github.com/pbhandar2/emory-phd ~/disk/emory-phd
cd ~/disk/emory-phd 
git submodule init
git submodule update 
pip3 install . --user
cd modules/Cydonia/
git checkout fast 
pip3 install . --user 
git submodule init 
git submodule update 
cd scripts/stack-distance
sudo make 

W="w66"
sudo mkdir -p /research2/mtc/cp_traces/pranav/block_traces/cp
sudo chown vishwa /research2/mtc/cp_traces/pranav/block_traces/cp
scp -P 8098 pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/block_traces/cp/${W}.csv /research2/mtc/cp_traces/pranav/block_traces/cp/${W}.csv

sudo mkdir -p /research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${W}/
sudo chown vishwa  /research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${W}/
scp -P 8098 -r pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${W}/* /research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${W}/




w=w64
scp -P 8098 /research2/mtc/cp_traces/pranav/sample_block_cache_traces/iat/cp/${w}/* pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/sample_block_cache_traces/iat/cp/${w}/


w=w64
scp -P 8098 /research2/mtc/cp_traces/pranav/block_cache_trace/cp/${w}.csv pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/block_cache_trace/cp/${w}.csv


w=w64
scp -P 8098 pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${w}/* /research2/mtc/cp_traces/pranav/sample_block_traces/iat/cp/${w}/


scp -P 8098 pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/block_traces/cp/${w}.csv /research2/mtc/cp_traces/pranav/block_traces/cp/${w}.csv

