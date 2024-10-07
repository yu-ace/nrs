package netty.redis.nrs.server.storage;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MemoryStorage {
    public Map<String, Index> map;
    public ByteBuffer buffer;
    private static final Integer TYPE_INTEGER = 0;
    private static final Integer TYPE_STRING = 1;

    private static final MemoryStorage memoryStorage = new MemoryStorage();
    private MemoryStorage(){}
    public static MemoryStorage getInstance(){
        return memoryStorage;
    }

    String dataPath;
    String mapPath;
    Integer totalMemory;

    private void readConfig(Map<String, Object> setting) {
        Iterator<String> iterator = setting.keySet().iterator();
        iterator.next();
        iterator.next();
        String configKey = iterator.next();
        Map<String,Object> config = (Map<String, Object>) setting.get(configKey);
        totalMemory = (Integer) config.get("totalMemory");
        dataPath = (String) config.get("dataPath");
        mapPath = (String) config.get("mapPath");
    }

    public void initMap(Map<String,Object> setting) {
        readConfig(setting);
        File file = new File(mapPath);
        map = new HashMap<>();
        if(file.exists()){
            try{
                FileInputStream fileInputStream = new FileInputStream(mapPath);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                map = (Map<String, Index>) objectInputStream.readObject();
            }catch (Exception e){
                e.getStackTrace();
            }
        }
    }

    public void init() throws Exception {
        File file = new File(dataPath);
        if(file.exists()){
            try(RandomAccessFile read = new RandomAccessFile(dataPath, "rw")) {
                long length = read.length();
                buffer = ByteBuffer.allocate((int) length);
                FileChannel channel = read.getChannel();
                channel.read(buffer);
                buffer.flip();
            }
        }else{
            buffer = ByteBuffer.allocate(1024*1024);
        }
    }

    public void saveMap() throws Exception {
        FileOutputStream fileOutputStream = new FileOutputStream(mapPath);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(map);
    }

    public void shutDown() throws Exception{
        RandomAccessFile file = new RandomAccessFile(dataPath,"rw");
        FileChannel channel = file.getChannel();
        buffer.flip();
        buffer.limit(buffer.capacity());
        channel.write(buffer);
        file.close();
    }

    public Record get(String key) {
        Record record;
        if(map.containsKey(key)){
            buffer.limit(totalMemory);
            Index index = map.get(key);
            buffer.position(index.getPosition());
            buffer.limit(index.getPosition() + index.getLength());
            ByteBuffer slice = buffer.slice();
            byte[] bytes = new byte[slice.remaining()];
            slice.get(bytes);
            record = new Record(key,bytes,map.get(key).getType());
        }else {
            record = new Record(key,null,null);
        }
        return record;
    }


    public void set(ByteBuffer byteBuffer, Map<String, Index> map, String key, Object value) {
        byte[] valueByte;
        String type;
        if(value instanceof String){
            valueByte = ((String) value).getBytes(StandardCharsets.UTF_8);
            type = "String";
        }else if(value instanceof Integer){
            valueByte = ByteBuffer.allocate(4).putInt((Integer) value).array();
            type = "Integer";
        }else{
            valueByte = processList((List<?>) value);
            type = "List";
        }
        Index index = getIndex(byteBuffer,valueByte, type);
        map.put(key,index);
    }

    public void set(ByteBuffer byteBuffer, Map<String, Index> map, String key, Record record) {
        Index index = getIndex(byteBuffer,record.getValue(), record.getType());
        map.put(key,index);
    }

    private Index getIndex(ByteBuffer buffer, byte[] valueByte, String type) {
        int valurLength = valueByte.length;
        buffer.position(0);
        int size = buffer.getInt();
        buffer.position(0);
        buffer.putInt(size + 1);
        int position = 4 + 8 * size;
        int startPosition;
        if(size == 0){
            startPosition = 1024*1024 - valurLength;
        }else {
            buffer.position(position - 8);
            int lastPosition = buffer.getInt();
            startPosition = lastPosition - valurLength;
        }
        buffer.position(position);
        buffer.putInt(startPosition);
        buffer.putInt(valurLength);
        buffer.position(startPosition);
        buffer.put(valueByte);

        return new Index(startPosition, valurLength, type);
    }

    private byte[] processList(List<?> value) {
        try{
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            dataOutputStream.writeInt(value.size());
            for(Object v:value){
                if(v instanceof String){
                    byte[] bytes = ((String) v).getBytes(StandardCharsets.UTF_8);
                    dataOutputStream.writeInt(TYPE_STRING);
                    dataOutputStream.writeInt(bytes.length);
                    dataOutputStream.write(bytes);
                }else {
                    dataOutputStream.writeInt(TYPE_INTEGER);
                    dataOutputStream.writeInt(4);
                    dataOutputStream.write((Integer) v);
                }
            }
            return byteArrayOutputStream.toByteArray();
        }catch (Exception e){
            e.getStackTrace();
        }
        return null;
    }

    public String incr(String key){
        Record record = get(key);
        if("Integer".equals(record.getType())){
            ByteBuffer byteBuffer = ByteBuffer.wrap(record.getValue());
            int value = byteBuffer.getInt();
            value++;
            set(buffer,map,key,value);
            return Integer.toString(value);
        }else if(record.getType() == null){
            set(buffer,map,key,1);
            return Integer.toString(1);
        }else {
            return "null";
        }
    }

    public String decr(String key){
        Record record = get(key);
        if("Integer".equals(record.getType())){
            ByteBuffer byteBuffer = ByteBuffer.wrap(record.getValue());
            int value = byteBuffer.getInt();
            value--;
            set(buffer,map,key,value);
            return Integer.toString(value);
        }else if(record.getType() == null){
            set(buffer,map,key,-1);
            return Integer.toString(-1);
        }else {
            return "null";
        }
    }

    public Set<String> keyList(){
        return map.keySet();
    }

    public String delete(String key){
        String response;
        if(!map.containsKey(key)){
            response = "key is null";
        }else {
            map.remove(key);
            response = "delete ok";
        }
        return response;
    }

    public Integer usedMemory(){
        int usedMemory;
        buffer.position(0);
        int size = buffer.getInt();
        if(size != 0){
            int index = 4 + 8 * (size - 1);
            buffer.position(index);
            int lastStartPosition = buffer.getInt();
            int userHeadPosition = 4 + 8 * size;
            usedMemory = totalMemory - lastStartPosition + userHeadPosition;
        }else {
            usedMemory = 0;
        }
        return usedMemory;
    }

    public Integer FreeMemory(){
        return totalMemory - usedMemory();
    }
}
