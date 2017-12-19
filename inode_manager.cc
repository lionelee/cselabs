#include <pthread.h>



#include "inode_manager.h"
#include <time.h>

/*StudentID: 515030910292*/
/*Name: Li Xinyu*/

// disk layer -----------------------------------------

using namespace std;

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE); 
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */

  blockid_t bid=0;
  char buf[BLOCK_SIZE];
  int num = BBLOCK(sb.nblocks);
  for(int i = BBLOCK(0); i < num; ++i){
      read_block(i, buf);
      for(int j = 0; j < BLOCK_SIZE; ++j){
          for(unsigned char mask = 0x80; mask > 0 && bid < sb.nblocks; mask = mask >> 1){
	          if((buf[j] & mask) == 0){
                buf[j] |= mask;
                write_block(i, buf);
                return bid;
            }
            ++ bid;
	      }
      }
  } 
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id), buf);
  int idx = (id % BPB) / 8;
  unsigned char mask = 1<<(7- (id%BPB)%8);
  if((buf[idx] & mask) == 0){
      printf("\tim: error! free not allocated block\n");
      return;
  }
  buf[idx] ^= mask;
  write_block(BBLOCK(id), buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM / 2;
  sb.nblocks = BLOCK_NUM / 2;
  sb.ninodes = INODE_NUM;
  
  alloc_block(); //alloc boot block
  alloc_block(); //alloc super block
  int bnum = sb.nblocks / BPB, inum = INODE_NUM / IPB;
  for(int i = 0; i < bnum; ++i){ //alloc bitmap for free blocks 
      alloc_block(); 
  }
  for(int j = 0; j < inum; ++j){ //alloc inode table  
      alloc_block();
    }
}

char 
parity(char c)
{
    char ret = c & 1;
    for(int i = 1; i < 8; ++i){
        ret ^= (c>>i);
    }
    return ret & 1;  
}
  
char 
encode(char c){
    char ret = c & 0xf; 
    char check_code = parity(ret & 0xd) << 6;
    check_code |= parity(ret & 0xb) << 5;
    check_code |= parity(ret & 0x7) << 3;
    char p3 = (c & 0x8) << 1;
    ret = (c&0x7) | (check_code | p3);
    ret |= (parity(ret)<<7);
    return ret;
}

char 
decode(char c){
  char flag = parity(c & 0x55);
  flag |= parity(c & 0x33)<<1;
  flag |= parity(c & 0x0f)<<2;
  char check = (c>>7) & 1;
  c &= 0x7f;
  switch(flag){
    case 0x1:case 0x2:case 0x4:
        if(parity(c) == check){ //two bit flip
            return 0x80;
        }
        break;
    case 0x3:
        c ^= 0x10;
        break;
    case 0x5:
        c ^= 0x4;
        break;
    case 0x6:
        c ^= 0x2;
        break;
    case 0x7:
        c ^= 0x1;
    default:break;
  }
  char ret = (c & 0x10) >> 1;
  ret |= (c & 0x7);
  return ret;  
}

void
block_manager::read_block(uint32_t id, char *buf)
{
//   char code[BLOCK_SIZE*4];
//   d->read_block(id*4, code);
//   d->read_block(id*4+1, code+BLOCK_SIZE);
//   d->read_block(id*4+2, code+BLOCK_SIZE*2);
//   d->read_block(id*4+3, code+BLOCK_SIZE*3);
//   for (int i = 0; i < BLOCK_SIZE; ++i) {
//       char high = decode(code[i]);
//       if(high == 0x80){
//           high = decode(code[i+BLOCK_SIZE*2]);
//           if(high == 0x80){       
//               printf("\tim: error! error can not be corrected\n");
//               exit(1);
//           }
//       }
//       char low  = decode(code[i+BLOCK_SIZE]);
//       if (low == 0x80) {
//           low  = decode(code[i+BLOCK_SIZE*3]);
//           if(low == 0x80){        
//               printf("\tim: error! error can not be corrected\n");
//               exit(1);
//           }
//       }
//       buf[i] = (high << 4) | low;
//   }  
//   write_block(id,buf); // update data
  char code[BLOCK_SIZE*2];
  d->read_block(id*2, code);
  d->read_block(id*2+1, code+BLOCK_SIZE);
  for (int i = 0; i < BLOCK_SIZE; ++i) {
      char high = decode(code[i*2]);
      char low  = decode(code[i*2+1]);
      if(high == 0x80 || low == 0x80){       
          printf("\tim: error! error can not be corrected\n");
          exit(1);
      }
      buf[i] = (high << 4) | low;
  }  
  write_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
//   char code[BLOCK_SIZE * 4];
//   for (int i = 0; i < BLOCK_SIZE; ++i) {
//       char low  = encode((buf[i] & 0xf));
//       char high = encode((buf[i] >> 4) & 0xf);
//       code[i] = high;
//       code[i+BLOCK_SIZE] = low;
//       code[i+BLOCK_SIZE*2] = high;
//       code[i+BLOCK_SIZE*3] = low;
//   }

  char code[BLOCK_SIZE*2];
  for (int i = 0; i < BLOCK_SIZE; ++i) {
      char low  = encode((buf[i] & 0xf));
      char high = encode((buf[i] >> 4) & 0xf);
      code[i*2] = high;
      code[i*2+1] = low;
  }

  d->write_block(id*2, code);
  d->write_block(id*2+1, code + BLOCK_SIZE);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
  pthread_mutex_init(&im_mutex, NULL);
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */

  uint32_t inum = 1;
  char buf[BLOCK_SIZE];
  uint32_t num = IBLOCK(INODE_NUM, bm->sb.nblocks);
  for(uint32_t i = IBLOCK(inum, bm->sb.nblocks); i < num; ++i){
      bm->read_block(i, buf);
      for(int j = 0; j < IPB; ++j){
          inode_t* ino = (inode_t*)buf + j;
          if(ino->type == 0){
              ino->type = type;
	          ino->size = 0;
              ino->atime = time(NULL);
	          ino->mtime = time(NULL);
              ino->ctime = time(NULL); 
              bm->write_block(i, buf);
              return inum;
          }
          ++inum;
      }
  }
  return -1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
   
  inode_t* ino = get_inode(inum);
  if(ino == NULL){
      printf("\tim: error! inode not found\n");
      return;
  }
  if(ino->type == 0){
      printf("\tim: error! inode freed before\n");
      return;
  }
  ino->type = 0;
  put_inode(inum, ino);

  free(ino);
  ino = NULL;
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t &inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */

  inode_t* ino = get_inode(inum);

  if(ino == NULL){
      printf("\tim: error! inode not found\n");
      exit(0);
  }
  
  char buf[BLOCK_SIZE];
  char* content = (char*)malloc(ino->size);
  char* content_out = content;
  unsigned int nblocks = (ino->size + BLOCK_SIZE -1) / BLOCK_SIZE;
  unsigned int len = 0, isize = ino->size;
  int num = MIN(nblocks, NDIRECT);
  for(int i = 0; i < num; ++i){
      bm->read_block(ino->blocks[i], buf);
      len = MIN(isize, BLOCK_SIZE);
      memcpy(content, buf, len);   
      content += len;
      isize -= len;
  }   
  int res = nblocks - NDIRECT;
  if(res > 0){
      char blocks_buf[BLOCK_SIZE];
      bm->read_block(ino->blocks[NDIRECT], blocks_buf);
      blockid_t* blocks = (blockid_t*)blocks_buf;
      for(int j = 0; j < res; ++j){
          bm->read_block(blocks[j], buf);
          len = MIN(isize, BLOCK_SIZE);
          memcpy(content, buf, len);
          content += len;
          isize -= len;
      }
  }

  *buf_out = content_out;
  *size = ino->size;

  ino->atime = time(NULL);
  ino->ctime = time(NULL);
  put_inode(inum, ino);

  free(ino);
  ino = NULL;
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  pthread_mutex_lock(&im_mutex);
  inode_t* ino = get_inode(inum);
  if(ino == NULL){
      printf("\tim: error! inode not found\n");
      exit(1);
  }

  unsigned int old_blocks = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int new_blocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  char blocks_buf[BLOCK_SIZE];
  blockid_t* blocks;
  if(new_blocks > MAXFILE){
      printf("\tim: error! file too large\n");
      exit(0);
  }

  //free all old blocks
  int num = MIN(old_blocks, NDIRECT);
  for(int i = 0; i < num; ++i){
     bm->free_block(ino->blocks[i]);
  }
  int res = old_blocks - NDIRECT;
  if(res > 0){  // free indirect blocks
      bm->read_block(ino->blocks[NDIRECT], blocks_buf);
      blocks = (blockid_t*)blocks_buf;
      for(int j = 0; j < res; ++j){
         bm->free_block(blocks[j]);
      }
      bm->free_block(ino->blocks[NDIRECT]);
  }

  //alloc new blocks and write
  uint32_t bid;
  unsigned int len = 0, isize = size;
  char content[BLOCK_SIZE];
  num = MIN(new_blocks, NDIRECT);
  for(int i = 0; i < num; ++i){
      bid = bm->alloc_block();
      ino->blocks[i] = bid;
      len = MIN(BLOCK_SIZE, isize);
      memcpy(content, buf, len);
      bm->write_block(bid, content);
      buf += len;
      isize -= len;
  }
  res = new_blocks - NDIRECT;
  if(res > 0){
      ino->blocks[NDIRECT] = bm->alloc_block();
      bm->read_block(ino->blocks[NDIRECT], blocks_buf);
      blocks = (blockid_t*)blocks_buf;
      for(int j = 0; j < res; ++j){
          bid = bm->alloc_block();
          blocks[j] = bid;
          len = MIN(BLOCK_SIZE, isize);
          memcpy(content, buf, len);
          bm->write_block(bid, content);
          buf += len;
          isize -= len;
      }
      bm->write_block(ino->blocks[NDIRECT], blocks_buf);
  }

  ino->atime = time(NULL);
  ino->mtime = time(NULL);
  ino->ctime = time(NULL);
  ino->size = size;

  put_inode(inum, ino);

  free(ino);
  ino = NULL;
  pthread_mutex_unlock(&im_mutex);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  
  inode_t* ino = get_inode(inum);

  if(ino == NULL){
      return;
  }
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
  
  unsigned int nblocks = (ino->size + BLOCK_SIZE -1) / BLOCK_SIZE;
  printf("%d free %d blocks\n", inum, nblocks);

  free(ino);
  ino = NULL;
  
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  
  inode_t* ino = get_inode(inum);
  if(ino == NULL){
      printf("\tim: error! inode not found\n");
      exit(0);
  }
  int nblock = (ino->size + BLOCK_SIZE -1) / BLOCK_SIZE;
  int num = MIN(nblock, NDIRECT);
  for(int i = 0; i < num; ++i){
      bm->free_block(ino->blocks[i]);
  }
  int res = nblock - NDIRECT;
  if(res > 0){
      char blocks_buf[BLOCK_SIZE];
      bm->read_block(ino->blocks[NINDIRECT], blocks_buf);
      blockid_t* blocks = (blockid_t*)blocks_buf;
      for(int j = 0; j < res; ++j){
          bm->free_block(blocks[j]);
      }
      bm->free_block(ino->blocks[NINDIRECT]);
  }
  free_inode(inum);
  free(ino);
  ino = NULL;
  return;
}


void 
inode_manager::commit()
{
  printf("\tim: commit version v%d\n", version);
  ++max_version;
  version = max_version;
}

void 
inode_manager::undo()
{
  if(version > 1){
    --version;
  } else {
    printf("\tim: error! no previous version.\n");
    return;
  }

  printf("\tim: back to version v%d\n", version);
}

void 
inode_manager::redo()
{
  if(version < max_version){
    ++version; 
  } else {
    printf("\tim: error! already latest version.\n");
    return;
  }

  printf("\tim: forward to version v%d\n", version);
}
