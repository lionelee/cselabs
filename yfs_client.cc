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

yfs_client::yfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
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
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

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

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
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

    string buf;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("setattr: get contents of %lld failed\n", ino);
        return r;
    }

    buf.resize(size,'\0');

    r = ec->put(ino, buf);
    if(r != OK){
        printf("setattr: update attr of %lld failed\n", ino);
    }
    
    return r;
}

int 
yfs_client::createUtil(inum parent, const char *name, uint32_t type, inum &ino_out)
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
    r = lookup(parent,name, found, ino_out);
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
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    return createUtil(parent, name, extent_protocol::T_FILE, ino_out);
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    return createUtil(parent, name, extent_protocol::T_DIR, ino_out);
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    if(!isdir(parent)){
        found = false;
        printf("lookup: %lld not a dir\n",  parent);
        r = IOERR;
        return r;
    } 

    string buf;
    r = ec->get(parent, buf);
    if(r != OK){
        printf("lookup: get contents of %lld failed\n", parent);
        found = false;
        return r;
    }

    /////////////////////////////////
    cout<<"entries: "<<buf<<endl;
    /////////////////////////////////

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
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    if(!isdir(dir)){
        printf("readdir: %lld not a dir\n",  dir);
        r = IOERR;
        return r;
    } 

    string buf;
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
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    string buf;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("read: get contents of %lld failed\n", ino);
        return r;
    }

    int len = buf.size();
    if(off < len){
        data = buf.substr(off, size);
    } else {
        data = "";
    }

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

    string buf;
    r = ec->get(ino, buf);
    if(r != OK){
        printf("write: get contents of %lld failed\n", ino);
        return r;
    }
    
    uint len = buf.size();
    string dataStr = string(data,size);
    if(len < off || len < (off+size)){
        buf.resize(off,'\0');
        buf.append(dataStr);
    } else {
        buf.replace(off, size, dataStr);
    }

    r = ec->put(ino, buf);
    if(r != OK){
        printf("write: write contents of %lld failed\n", ino);
        return r;
    }

    bytes_written = size;
    return r;
}

int 
yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    list<dirent> entries;
    r = readdir(parent, entries);
    if(r != OK){
        printf("unlink: get contents of %lld failed\n", parent);
        return r;
    }

    bool flag = false;
    string res = "", fname = string(name);
    list<dirent>::iterator it = entries.begin();
    for(; it != entries.end(); ++it){
        if(it->name == fname){
            flag = true;
            r = ec->remove(it->inum);
            if(r != OK){
                printf("unlink: remove inode %lld failed\n", it->inum);
                return r;
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
            return r;
        }
    } else {
        printf("unlink: %s not in dir %lld\n", name, parent);
        r = NOENT;
    }
    return r;
}

int 
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = createUtil(parent, name, extent_protocol::T_SYMLINK, ino_out);
    if(r == OK){
        r = ec->put(ino_out, string(link));
        if(r != OK){
            printf("symlink: write contents of %lld failed\n", ino_out);
        }
    }
    return r;
}

int 
yfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;
    if(!issymlink(ino)){
        printf("readlink: %lld not a symbolic link\n", ino);
        r = IOERR;
        return r;
    } 
    
    r = ec->get(ino, link);
    if (r != OK) {
        printf("readlink: link not exist\n");
    }
    return r;
}

