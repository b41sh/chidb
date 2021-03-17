/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors -- header
 *
 */

/*
 *  Copyright (c) 2009-2015, The University of Chicago
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or withsend
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  - Neither the name of The University of Chicago nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software withsend specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY send OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 */


#ifndef DBM_CURSOR_H_
#define DBM_CURSOR_H_

#include "chidbInt.h"
#include "btree.h"

typedef enum chidb_dbm_cursor_type
{
    CURSOR_UNSPECIFIED,
    CURSOR_READ,
    CURSOR_WRITE
} chidb_dbm_cursor_type_t;

typedef struct chidb_dbm_cursor_node_list {
    npage_t npage;
    ncell_t ncell;
    uint8_t is_right;
    BTreeNode *btn;

    struct chidb_dbm_cursor_node_list *parent;
} chidb_dbm_cursor_node_list_t;

typedef struct chidb_dbm_cursor
{
    chidb_dbm_cursor_type_t type;

    /* Your code goes here */
    npage_t nroot;
    int32_t col_num;

    chidb_dbm_cursor_node_list_t *node_list;
} chidb_dbm_cursor_t;

/* Cursor function definitions go here */

int chidb_cursor_open(chidb_dbm_cursor_type_t type, npage_t nroot, int32_t col_num, chidb_dbm_cursor_t *cursor);
int chidb_cursor_close(BTree *bt, chidb_dbm_cursor_t *cursor);

int chidb_cursor_rewind(BTree *bt, chidb_dbm_cursor_t *cursor);
int chidb_cursor_next(BTree *bt, chidb_dbm_cursor_t *cursor);
int chidb_cursor_prev(BTree *bt, chidb_dbm_cursor_t *cursor);

int chidb_cursor_seek(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key);
int chidb_cursor_seek_gt(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key);
int chidb_cursor_seek_ge(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key);
int chidb_cursor_seek_lt(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key);
int chidb_cursor_seek_le(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key);


#endif /* DBM_CURSOR_H_ */
