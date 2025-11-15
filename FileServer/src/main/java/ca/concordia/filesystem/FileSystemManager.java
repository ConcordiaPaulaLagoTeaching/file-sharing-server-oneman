package ca.concordia.filesystem;
import ca.concordia.filesystem.datastructures.FEntry;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FileSystemManager {
    private static final int MAX_FILES = 5;
    private static final int MAX_BLOCKS = 10;
    private static final int BLOCK_SIZE = 128;
    private static final int META_BLOCK_INDEX = 0;
    private final RandomAccessFile disk;
    private final FEntry[] entries = new FEntry[MAX_FILES];

    public FileSystemManager(String filename, int totalSize) throws Exception {
        File f = new File(filename);
        this.disk = new RandomAccessFile(f, "rw");
        if (disk.length() == 0) {
            disk.setLength(totalSize);
            initEmptyFileSystem();
            writeMetadata();
        } else {
            loadMetadata();
        }
    }

    public synchronized void createFile(String fileName) throws Exception {//creates new file if it doesnt already exist 
        if (fileName == null || fileName.isEmpty()) {
            throw new Exception("filename is empty");
        }
        if (fileName.length() > 11) {
            throw new Exception("filename too long");
        }
        int existingIndex = findEntryIndex(fileName);
        if (existingIndex != -1) {
            return;
        }

        int freeIndex = -1;
        for (int i = 0; i < MAX_FILES; i++) {
            if (entries[i] == null) {
                freeIndex = i;
                break;
            }
        }
        if (freeIndex == -1) {
            throw new Exception("no free file entries");
        }

        entries[freeIndex] = new FEntry(fileName, (short) 0, (short) -1);
        writeMetadata();
    }

    public synchronized void deleteFile(String fileName) throws Exception { //to delete a file 
        int idx = findEntryIndex(fileName);
        if (idx == -1) {
            throw new Exception("file " + fileName + " does not exist");
        }

        FEntry e = entries[idx];
        int size = e.getFilesize();
        int firstBlock = e.getFirstBlock();

        if (firstBlock >= 1 && size > 0) {
            int blocks = blocksNeeded(size);
            byte[] zeros = new byte[BLOCK_SIZE];
            for (int i = 0; i < blocks; i++) {
                long pos = (long) (firstBlock + i) * BLOCK_SIZE;
                disk.seek(pos);
                disk.write(zeros);
            }
        }

        entries[idx] = null;
        writeMetadata();
    }

    public synchronized void writeFile(String fileName, byte[] contents) throws Exception {//To overwrite file content 
        int idx = findEntryIndex(fileName);
        if (idx == -1) {
            throw new Exception("file " + fileName + " does not exist");
        }
        if (contents == null) {
            contents = new byte[0];
        }

        int size = contents.length;
        int neededBlocks = blocksNeeded(size);

        if (neededBlocks > MAX_BLOCKS - 1) { // data blocks are 1..9
            throw new Exception("file too large for filesystem");
        }

        boolean[] used = computeUsedBlocks(entries[idx]);
        int startBlock = findContiguousFreeRun(used, neededBlocks);

        if (neededBlocks > 0 && startBlock == -1) {
            throw new Exception("not enough contiguous space");
        }

        int offset = 0;
        for (int i = 0; i < neededBlocks; i++) {
            long pos = (long) (startBlock + i) * BLOCK_SIZE;
            disk.seek(pos);

            int remaining = size - offset;
            int toWrite = Math.min(remaining, BLOCK_SIZE);

            disk.write(contents, offset, toWrite);

            if (toWrite < BLOCK_SIZE) {
                byte[] pad = new byte[BLOCK_SIZE - toWrite];
                disk.write(pad);
            }

            offset += toWrite;
        }

        FEntry e = entries[idx];
        e.setFilesize((short) size);
        e.setFirstBlock((short) (neededBlocks == 0 ? -1 : startBlock));

        writeMetadata();
    }
    public synchronized byte[] readFile(String fileName) throws Exception {//To read file contents 
        int idx = findEntryIndex(fileName);
        if (idx == -1) {
            throw new Exception("file " + fileName + " does not exist");
        }

        FEntry e = entries[idx];
        int size = e.getFilesize();

        if (size <= 0) {
            return new byte[0];
        }

        int firstBlock = e.getFirstBlock();
        if (firstBlock < 1) {
            throw new Exception("corrupted metadata for file " + fileName);
        }

        int blocks = blocksNeeded(size);
        byte[] result = new byte[size];
        int offset = 0;

        for (int i = 0; i < blocks; i++) {
            long pos = (long) (firstBlock + i) * BLOCK_SIZE;
            disk.seek(pos);

            int remaining = size - offset;
            int toRead = Math.min(remaining, BLOCK_SIZE);

            disk.readFully(result, offset, toRead);
            offset += toRead;
        }

        return result;
    }

    public synchronized String[] listFiles() {//List every file by its name 
        List<String> names = new ArrayList<>();
        for (FEntry e : entries) {
            if (e != null) {
                names.add(e.getFilename());
            }
        }
        return names.toArray(new String[0]);
    }

    //PRIVATE HANDLERS: 
    private void initEmptyFileSystem() throws IOException {
        disk.seek(0);
        byte[] zeros = new byte[BLOCK_SIZE];
        disk.write(zeros);  // clear metadata block
    }
    private int findEntryIndex(String name) {
        for (int i = 0; i < MAX_FILES; i++) {
            if (entries[i] != null && entries[i].getFilename().equals(name)) {
                return i;
            }
        }
        return -1;
    }
    private int blocksNeeded(int size) {
        if (size <= 0) return 0;
        return (size + BLOCK_SIZE - 1) / BLOCK_SIZE;  // ceil
    }

    private boolean[] computeUsedBlocks(FEntry ignore) {
        boolean[] used = new boolean[MAX_BLOCKS];
        used[META_BLOCK_INDEX] = true;

        for (FEntry e : entries) {
            if (e == null || e == ignore) continue;

            int size = e.getFilesize();
            int first = e.getFirstBlock();
            if (size <= 0 || first < 1) continue;

            int blocks = blocksNeeded(size);
            for (int i = 0; i < blocks; i++) {
                int idx = first + i;
                if (idx >= 0 && idx < MAX_BLOCKS) {
                    used[idx] = true;
                }
            }
        }
        return used;
    }

    private int findContiguousFreeRun(boolean[] used, int count) {//find blocks the same size as count 
        if (count == 0) return -1;

        int start = 1;
        while (start + count <= MAX_BLOCKS) {
            boolean ok = true;
            for (int i = 0; i < count; i++) {
                if (used[start + i]) {
                    ok = false;
                    start = start + i + 1;
                    break;
                }
            }
            if (ok) return start;
        }
        return -1;
    }

    private void writeMetadata() throws IOException {
        disk.seek(0);
        for (int i = 0; i < MAX_FILES; i++) {
            FEntry e = entries[i];
            if (e == null) {
                byte[] empty = new byte[11];
                disk.write(empty);
                disk.writeShort(0);
                disk.writeShort(-1);
            } else {
                byte[] nameBytes = e.getFilename().getBytes(StandardCharsets.UTF_8);
                byte[] buf = new byte[11];
                int len = Math.min(nameBytes.length, 11);
                System.arraycopy(nameBytes, 0, buf, 0, len);
                disk.write(buf);
                disk.writeShort(e.getFilesize());
                disk.writeShort(e.getFirstBlock());
            }
        }
        disk.getFD().sync();
    }
    
    private void loadMetadata() throws IOException {
        disk.seek(0);
        for (int i = 0; i < MAX_FILES; i++) {
            byte[] nameBuf = new byte[11];
            disk.readFully(nameBuf);
            short size = disk.readShort();
            short firstBlock = disk.readShort();

            String name = new String(nameBuf, StandardCharsets.UTF_8).trim();
            if (name.isEmpty()) {
                entries[i] = null;
            } else {
                entries[i] = new FEntry(name, size, firstBlock);
            }
        }
    }
}