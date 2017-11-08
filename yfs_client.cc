// yfs client.  implements FS operations using extent and lock server
/*StudentID: 515030910292*/
/*Name: Li Xinyu*/

#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;
    
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("isdir: error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a directory\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;
    
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("issymlink: error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symbolic link\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symbolic link\n", inum);
    return false;
}


int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
	lc->acquire(inum);
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto gf_release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

gf_release:
	lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
	lc->acquire(inum);
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto gd_release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

gd_release:
	lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    
	lc->acquire(ino);
    string buf;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("setattr: get contents of %lld failed\n", ino);
        goto sa_release;
    }

    buf.resize(size,'\0');

    r = ec->put(ino, buf);
    if(r != OK){
        printf("setattr: update attr of %lld failed\n", ino);
    }
    
 sa_release:   
	lc->release(ino);
    return r;
}

int 
yfs_client::local_create(inum parent, const char *name, uint32_t type, inum &ino_out)
{
    int r = OK;
    bool found = false;
    const char* tname;
    switch(type){
        case extent_protocol::T_DIR:
            tname = "directory";
            break;
        case extent_protocol::T_FILE:
            tname = "file";
            break;
        case extent_protocol::T_SYMLINK:
            tname = "symbolic link";
        default: break;
    }
    r = local_lookup(parent,name, found, ino_out);
    if(found == true){
        printf("%s named %s already exist\n", tname, name);
    }
    else if(r == NOENT && found == false){
        r = ec->create(type, ino_out);
        if(r != OK){
            printf("create %s failed\n", tname);
            return r;
        }
        string buf="";
        ec->get(parent, buf);
        string buf_ent = string(name)+"/"+filename(ino_out)+",";

        buf.append(buf_ent);
        r = ec->put(parent, buf);
        if(r != OK){
            printf("update parent dir failed\n");
            return r;
        }
    } 
    else r = IOERR;

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

	lc->acquire(parent);
    r = local_create(parent, name, extent_protocol::T_FILE, ino_out);
	lc->release(parent);
	return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	
	lc->acquire(parent);
    r = local_create(parent, name, extent_protocol::T_DIR, ino_out);
    lc->release(parent);
    return r;
}

int 
yfs_client::local_lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    
    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    string buf;
    if(!isdir(parent)){
        found = false;
        printf("lookup: %lld not a dir\n",  parent);
        r = IOERR;
        return r;
    } 

    r = ec->get(parent, buf);
    if(r != OK){
        printf("lookup: get contents of %lld failed\n", parent);
        found = false;
        return r;
    }

    size_t i = 0, len = buf.length();
    while(i < len){
        string buf_ent = "";
        while(buf[i]!=','){
            buf_ent += buf[i];
            i++;
        }
        size_t sep = buf_ent.find('/');
        string file_name = buf_ent.substr(0, sep);

        if(file_name == string(name)){
            ino_out = n2i(buf_ent.substr(sep+1, buf_ent.length()));
            found = true;
            r = EXIST;
            return r;
        }
        while(buf[i]==',')i++;
    }
    r = NOENT;
    found = false;
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    lc->acquire(parent);
    r = local_lookup(parent, name, found, ino_out);
	lc->release(parent);
    return r;
}

int 
yfs_client::local_readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    
    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    string buf;
    if(!isdir(dir)){
        printf("readdir: %lld not a dir\n",  dir);
        r = IOERR;
        return r;
    } 

    r = ec->get(dir, buf);
    if(r != OK){
        printf("lookup: get contents of %lld failed\n", dir);
        return r;
    }

    size_t i = 0, len = buf.length();
    while(i < len){
        string buf_ent = "";
        while(buf[i]!=','){
            buf_ent += buf[i];
            i++;
        }
        size_t sep = buf_ent.find('/');
        string file_name = buf_ent.substr(0, sep);
        inum ino = n2i(buf_ent.substr(sep+1, buf_ent.length()));
        
        struct dirent dent = {file_name, ino};
        list.push_back(dent);
        while(buf[i]==',')i++;
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    lc->acquire(dir);
    r = local_readdir(dir, list);
	lc->release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

	lc->acquire(ino);
    string buf;
    off_t len = 0;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("read: get contents of %lld failed\n", ino);
        goto rrelease;
    }

    len = buf.size();
    if(off < len){
        data = buf.substr(off, size);
    } else {
        data = "";
    }

rrelease:
	lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    lc->acquire(ino);
    string buf, dataStr;
    size_t len=0;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("write: get contents of %lld failed\n", ino);
        goto wrelease;
    }
    
    len = buf.size();
    dataStr = string(data,size);
    if(((off_t)len) < off || len < (off+size)){
        buf.resize(off,'\0');
        buf.append(dataStr);
    } else {
        buf.replace(off, size, dataStr);
    }

    r = ec->put(ino, buf);
    if(r != OK){
        printf("write: write contents of %lld failed\n", ino);
        goto wrelease;
    }
    bytes_written = size;

wrelease:
	lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

	lc->acquire(parent);
    list<dirent> entries;
    list<dirent>::iterator it;
    string res="", fname = string(name);
    bool flag = false;
    r = local_readdir(parent, entries);
    if(r != OK){
        printf("unlink: get contents of %lld failed\n", parent);
        goto ul_release;;
    }
    
    for(it = entries.begin(); it != entries.end(); ++it){
        if(it->name == fname){
            flag = true;
            r = ec->remove(it->inum);
            if(r != OK){
                printf("unlink: remove inode %lld failed\n", it->inum);
                goto ul_release;;
            }
        } else {
            // if(it->name != "")
            res += it->name + "/" + filename(it->inum) + ",";
        }
    }
    
    if(flag){
        r = ec->put(parent, res);
        if(r != OK){
            printf("unlink: update parent dir %lld failed\n", parent);
            goto ul_release;;
        }
    } else {
        printf("unlink: %s not in dir %lld\n", name, parent);
        r = NOENT;
    }
    
ul_release:
	lc->release(parent);
    return r;
}

int 
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
	lc->acquire(parent);
    int r = local_create(parent, name, extent_protocol::T_SYMLINK, ino_out);
    if(r == OK){
        r = ec->put(ino_out, string(link));
        if(r != OK){
            printf("symlink: write contents of %lld failed\n", ino_out);
        }
    }
    lc->release(parent);
    return r;
}

int 
yfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;
    lc->acquire(ino);
    if(!issymlink(ino)){
        printf("readlink: %lld not a symbolic link\n", ino);
        r = IOERR;
        goto rl_release;
    } 
    
    r = ec->get(ino, link);
    if (r != OK) {
        printf("readlink: link not exist\n");
    }

rl_release:
    lc->release(ino);
    return r;
}

