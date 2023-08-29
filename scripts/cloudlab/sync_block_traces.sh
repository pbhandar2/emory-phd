mkdir ~/disk 
sudo mkfs -t ext4 /dev/sdb
sudo mount /dev/sdb ~/disk

sudo apt-get update 
sudo apt install -y python3-pip clang 
sudo chown vishwa ~/disk

W="w92"
sudo mkdir -p /research2/mtc/cp_traces/pranav/block_traces/cp
sudo chown vishwa /research2/mtc/cp_traces/pranav/block_traces/cp
scp -P 8098 pranav@mjolnir.mathcs.emory.edu:/research2/mtc/cp_traces/pranav/block_traces/cp/${W}.csv /research2/mtc/cp_traces/pranav/block_traces/cp/${W}.csv

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
cd scripts/stack-disktance
sudo make 
