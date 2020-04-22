port1=9000
port2=9200
port3=9300

echo 'Running node on port:' $port1
nohup python3 Node_vishal_2.py $port1 &
sleep 1

echo
echo 'Running node on port:' $port2
nohup python3 Node_vishal_2.py $port2 $port1 &
sleep 4

echo
echo 'Running node on port:' $port3
nohup python3 Node_vishal_2.py $port3 $port1 &
sleep 8

key1='good'
val1='world'
key2='corona'
val2='virus'

echo
eval echo $(echo 'Inserting $key1:$val1 on $port2' )
x="$(echo "insert|$key1:$val1" | nc localhost $port2)"
sleep 2
echo $x

echo
eval echo $(echo 'Searching key=$key1 on $port3' )
x="$(echo "search|$key1" | nc localhost $port3)"
sleep 2
echo $x

echo
eval echo $(echo 'Inserting $key2:$val2 on $port3' )
x="$(echo "insert|$key2:$val2" | nc localhost $port3)"
sleep 2
echo $x

eval echo $(echo 'Searching key=$key2 on $port1' )
x="$(echo "search|$key2" | nc localhost $port1)"
sleep 2
echo $x


pkill -f Node_vishal_2.py