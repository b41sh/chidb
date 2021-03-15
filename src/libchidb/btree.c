/*
 *  chidb - a didactic relational database management system
 *
 * This module contains functions to manipulate a B-Tree file. In this context,
 * "BTree" refers not to a single B-Tree but to a "file of B-Trees" ("chidb
 * file" and "file of B-Trees" are essentially equivalent terms).
 *
 * However, this module does *not* read or write to the database file directly.
 * All read/write operations must be done through the pager module.
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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <chidb/log.h>
#include "chidbInt.h"
#include "btree.h"
#include "record.h"
#include "pager.h"
#include "util.h"


/* Open a B-Tree file
 *
 * This function opens a database file and verifies that the file
 * header is correct. If the file is empty (which will happen
 * if the pager is given a filename for a file that does not exist)
 * then this function will (1) initialize the file header using
 * the default page size and (2) create an empty table leaf node
 * in page 1.
 *
 * Parameters
 * - filename: Database file (might not exist)
 * - db: A chidb struct. Its bt field must be set to the newly
 *       created BTree.
 * - bt: An out parameter. Used to return a pointer to the
 *       newly created BTree.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECORRUPTHEADER: Database file contains an invalid header
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_open(const char *filename, chidb *db, BTree **bt)
{
    /* Your code goes here */
    int ret;
    uint16_t page_size = DEFAULT_PAGE_SIZE;
    //uint32_t file_change_counter = 0;
    //uint32_t schema_version = 0;
    uint32_t page_cache_size = DEFAULT_PAGE_CACHE_SIZE;
    //uint32_t user_cookie = 0;
    //npage_t n_pages = 1;

    uint16_t magic_num_1 = 0x0101;
    uint32_t magic_num_2 = 0x00402020;
    uint32_t magic_num_3 = 0x00;
    uint32_t magic_num_4 = 0x00;
    uint32_t magic_num_5 = 0x01;
    uint32_t magic_num_6 = 0x00;
    uint32_t magic_num_7 = 0x01;
    uint32_t magic_num_8 = 0x00;

    uint8_t arr2[2];
    uint8_t arr4[4];

    Pager *pager;
    if ((ret = chidb_Pager_open(&pager, filename)) != CHIDB_OK) {
        return ret;
    }
    uint8_t *buf = malloc(HEADER_BUF_SIZE);
    if (buf == NULL) {
        return CHIDB_ENOMEM;
    }
    ret = chidb_Pager_readHeader(pager, buf);
    if (ret == CHIDB_NOHEADER) {
        MemPage *memPage = malloc(sizeof(MemPage));
        if (memPage == NULL) {
            return CHIDB_ENOMEM;
        }
        if ((ret = chidb_Pager_allocatePage(pager, &memPage->npage)) != CHIDB_OK) {
            return ret;
        }
        memPage->data = malloc(page_size);
        if (memPage->data == NULL) {
            return CHIDB_ENOMEM;
        }
        memset(memPage->data, '\0', page_size);
        memcpy(&memPage->data[0], "SQLite format 3\0", MAGIC_BUF_SIZE);

        arr2[0] = (page_size >> 8) & 0xff;
        arr2[1] = page_size & 0xff;
        memcpy(&memPage->data[PAGE_SIZE_OFFSET], &arr2, sizeof(uint16_t));

        arr4[0] = (page_cache_size >> 24) & 0xff;
        arr4[1] = (page_cache_size >> 16) & 0xff;
        arr4[2] = (page_cache_size >> 8) & 0xff;
        arr4[3] = page_cache_size & 0xff;
        memcpy(&memPage->data[PAGE_CACHE_SIZE_OFFSET], &arr4, sizeof(uint32_t));

        arr2[0] = (magic_num_1 >> 8) & 0xff;
        arr2[1] = magic_num_1 & 0xff;
        memcpy(&memPage->data[MAGIC_NUM_1_OFFSET], &arr2, sizeof(uint16_t));

        arr4[0] = (magic_num_2 >> 24) & 0xff;
        arr4[1] = (magic_num_2 >> 16) & 0xff;
        arr4[2] = (magic_num_2 >> 8) & 0xff;
        arr4[3] = magic_num_2 & 0xff;
        memcpy(&memPage->data[MAGIC_NUM_2_OFFSET], &arr4, sizeof(uint32_t));

        arr4[0] = (magic_num_5 >> 24) & 0xff;
        arr4[1] = (magic_num_5 >> 16) & 0xff;
        arr4[2] = (magic_num_5 >> 8) & 0xff;
        arr4[3] = magic_num_5 & 0xff;
        memcpy(&memPage->data[MAGIC_NUM_5_OFFSET], &arr4, sizeof(uint32_t));

        arr4[0] = (magic_num_7 >> 24) & 0xff;
        arr4[1] = (magic_num_7 >> 16) & 0xff;
        arr4[2] = (magic_num_7 >> 8) & 0xff;
        arr4[3] = magic_num_7 & 0xff;
        memcpy(&memPage->data[MAGIC_NUM_7_OFFSET], &arr4, sizeof(uint32_t));

        uint8_t type = PGTYPE_TABLE_LEAF;
        memcpy(&memPage->data[HEADER_OFFSET + PGHEADER_PGTYPE_OFFSET], &type, sizeof(uint8_t));

        arr2[0] = ((HEADER_OFFSET + LEAFPG_CELLSOFFSET_OFFSET) >> 8) & 0xff;
        arr2[1] = (HEADER_OFFSET + LEAFPG_CELLSOFFSET_OFFSET) & 0xff;
        memcpy(&memPage->data[HEADER_OFFSET + PGHEADER_FREE_OFFSET], &arr2, sizeof(uint16_t));

        arr2[0] = (0 >> 8) & 0xff;
        arr2[1] = 0 & 0xff;
        memcpy(&memPage->data[HEADER_OFFSET + PGHEADER_NCELLS_OFFSET], &arr2, sizeof(uint16_t));

        arr2[0] = (page_size >> 8) & 0xff;
        arr2[1] = page_size & 0xff;
        memcpy(&memPage->data[HEADER_OFFSET + PGHEADER_CELL_OFFSET], &arr2, sizeof(uint16_t));

        pager->page_size = page_size;
        if ((ret = chidb_Pager_writePage(pager, memPage)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Pager_releaseMemPage(pager, memPage)) != CHIDB_OK) {
            return ret;
        }
    } else {
        if(strncmp((char*)buf, "SQLite format 3\0", MAGIC_BUF_SIZE) != 0) {
            return CHIDB_ECORRUPTHEADER;
        }
        page_size = (buf[PAGE_SIZE_OFFSET] << 8) | buf[PAGE_SIZE_OFFSET + 1];

        page_cache_size = (buf[PAGE_CACHE_SIZE_OFFSET] << 24) | (buf[PAGE_CACHE_SIZE_OFFSET + 1] << 16) |
            (buf[PAGE_CACHE_SIZE_OFFSET + 2] << 8) | buf[PAGE_CACHE_SIZE_OFFSET + 3];

        if (page_cache_size != DEFAULT_PAGE_CACHE_SIZE) {
            return CHIDB_ECORRUPTHEADER;
        }

        magic_num_1 = (buf[MAGIC_NUM_1_OFFSET] << 8) | buf[MAGIC_NUM_1_OFFSET + 1];
        magic_num_2 = (buf[MAGIC_NUM_2_OFFSET] << 24) | (buf[MAGIC_NUM_2_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_2_OFFSET + 2] << 8) | buf[MAGIC_NUM_2_OFFSET + 3];
        magic_num_3 = (buf[MAGIC_NUM_3_OFFSET] << 24) | (buf[MAGIC_NUM_3_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_3_OFFSET + 2] << 8) | buf[MAGIC_NUM_3_OFFSET + 3];
        magic_num_4 = (buf[MAGIC_NUM_4_OFFSET] << 24) | (buf[MAGIC_NUM_4_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_4_OFFSET + 2] << 8) | buf[MAGIC_NUM_4_OFFSET + 3];
        magic_num_5 = (buf[MAGIC_NUM_5_OFFSET] << 24) | (buf[MAGIC_NUM_5_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_5_OFFSET + 2] << 8) | buf[MAGIC_NUM_5_OFFSET + 3];
        magic_num_6 = (buf[MAGIC_NUM_6_OFFSET] << 24) | (buf[MAGIC_NUM_6_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_6_OFFSET + 2] << 8) | buf[MAGIC_NUM_6_OFFSET + 3];
        magic_num_7 = (buf[MAGIC_NUM_7_OFFSET] << 24) | (buf[MAGIC_NUM_7_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_7_OFFSET + 2] << 8) | buf[MAGIC_NUM_7_OFFSET + 3];
        magic_num_8 = (buf[MAGIC_NUM_8_OFFSET] << 24) | (buf[MAGIC_NUM_8_OFFSET + 1] << 16) |
            (buf[MAGIC_NUM_8_OFFSET + 2] << 8) | buf[MAGIC_NUM_8_OFFSET + 3];

        if (magic_num_1 != DEFAULT_MAGIC_NUM_1) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_2 != DEFAULT_MAGIC_NUM_2) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_3 != DEFAULT_MAGIC_NUM_3) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_4 != DEFAULT_MAGIC_NUM_4) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_5 != DEFAULT_MAGIC_NUM_5) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_6 != DEFAULT_MAGIC_NUM_6) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_7 != DEFAULT_MAGIC_NUM_7) {
            return CHIDB_ECORRUPTHEADER;
        }
        if (magic_num_8 != DEFAULT_MAGIC_NUM_8) {
            return CHIDB_ECORRUPTHEADER;
        }
        pager->page_size = page_size;
        if ((ret = chidb_Pager_getRealDBSize(pager, &pager->n_pages)) != CHIDB_OK) {
            return ret;
        }
    }

    *bt = malloc(sizeof(Btree));
    if (*bt == NULL) {
        return CHIDB_ENOMEM;
    }
    (*bt)->db = db;
    (*bt)->pager = pager;
    db->bt = *bt;

    return CHIDB_OK;
}


/* Close a B-Tree file
 *
 * This function closes a database file, freeing any resource
 * used in memory, such as the pager.
 *
 * Parameters
 * - bt: B-Tree file to close
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_close(BTree *bt)
{
    /* Your code goes here */
    int ret;
    if ((ret = chidb_Pager_close(bt->pager)) != CHIDB_OK) {
        return ret;
    }
    free(bt);

    return CHIDB_OK;
}


/* Loads a B-Tree node from disk
 *
 * Reads a B-Tree node from a page in the disk. All the information regarding
 * the node is stored in a BTreeNode struct (see header file for more details
 * on this struct). *This is the only function that can allocate memory for
 * a BTreeNode struct*. Always use chidb_Btree_freeMemNode to free the memory
 * allocated for a BTreeNode (do not use free() directly on a BTreeNode variable)
 * Any changes made to a BTreeNode variable will not be effective in the database
 * until chidb_Btree_writeNode is called on that BTreeNode.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Page of node to load
 * - btn: Out parameter. Used to return a pointer to newly creater BTreeNode
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EPAGENO: The provided page number is not valid
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_getNodeByPage(BTree *bt, npage_t npage, BTreeNode **btn)
{
    /* Your code goes here */
    if (npage > bt->pager->n_pages || npage < 1) {
        return CHIDB_EPAGENO;
    }
    int ret;
    MemPage *mem_page;
    if ((ret = chidb_Pager_readPage(bt->pager, npage, &mem_page)) != CHIDB_OK) {
        return ret;
    }
    int off = 0;
    if (npage == 1) {
        off += HEADER_BUF_SIZE;
    }
    *btn = malloc(sizeof(BTreeNode));
    if (*btn == NULL) {
        return CHIDB_ENOMEM;
    }
    (*btn)->page = mem_page;
    (*btn)->type = mem_page->data[off + 0];
    (*btn)->free_offset = (mem_page->data[off + 1] << 8) |
                            mem_page->data[off + 2];
    (*btn)->n_cells = (mem_page->data[off + 3] << 8) |
                            mem_page->data[off + 4];
    (*btn)->cells_offset = (mem_page->data[off + 5] << 8) |
                            mem_page->data[off + 6];
    if ((*btn)->type == PGTYPE_TABLE_INTERNAL || (*btn)->type == PGTYPE_INDEX_INTERNAL) {
        (*btn)->right_page = (mem_page->data[off + 8] << 24) |
                            (mem_page->data[off + 9] << 16) |
                            (mem_page->data[off + 10] << 8) |
                            mem_page->data[off + 11];
        (*btn)->celloffset_array = mem_page->data + off + INTPG_CELLSOFFSET_OFFSET;
    } else {
        (*btn)->right_page = 0;
        (*btn)->celloffset_array = mem_page->data + off + LEAFPG_CELLSOFFSET_OFFSET;
    }

    return CHIDB_OK;
}


/* Frees the memory allocated to an in-memory B-Tree node
 *
 * Frees the memory allocated to an in-memory B-Tree node, and
 * the in-memory page returned by the pages (stored in the
 * "page" field of BTreeNode)
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to free
 *
 * Return
 * - CHIDB_OK: Operation successful
 */
int chidb_Btree_freeMemNode(BTree *bt, BTreeNode *btn)
{
    /* Your code goes here */
    int ret = chidb_Pager_releaseMemPage(bt->pager, btn->page);
    free(btn);
    if (ret != CHIDB_OK) {
        return ret;
    }
    return CHIDB_OK;
}


/* Create a new B-Tree node
 *
 * Allocates a new page in the file and initializes it as a B-Tree node.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Out parameter. Returns the number of the page that
 *          was allocated.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_newNode(BTree *bt, npage_t *npage, uint8_t type)
{
    /* Your code goes here */
    int ret;
    if ((ret = chidb_Pager_allocatePage(bt->pager, npage)) != CHIDB_OK) {
        return ret;
    }
    return chidb_Btree_initEmptyNode(bt, *npage, type);
}


/* Initialize a B-Tree node
 *
 * Initializes a database page to contain an empty B-Tree node. The
 * database page is assumed to exist and to have been already allocated
 * by the pager.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage: Database page where the node will be created.
 * - type: Type of B-Tree node (PGTYPE_TABLE_INTERNAL, PGTYPE_TABLE_LEAF,
 *         PGTYPE_INDEX_INTERNAL, or PGTYPE_INDEX_LEAF)
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_initEmptyNode(BTree *bt, npage_t npage, uint8_t type)
{
    /* Your code goes here */
    int ret;
    MemPage *mem_page;
    if ((ret = chidb_Pager_readPage(bt->pager, npage, &mem_page)) != CHIDB_OK) {
        return ret;
    }
    mem_page->npage = npage;

    int page_off = 0;
    if (npage == 1) {
        page_off = HEADER_OFFSET;
    }
    uint16_t free_offset;
    ncell_t n_cells = 0;
    uint16_t cells_offset = bt->pager->page_size;
    //npage_t right_page;
    if (type == PGTYPE_TABLE_INTERNAL || type == PGTYPE_INDEX_INTERNAL) {
        free_offset = page_off + INTPG_CELLSOFFSET_OFFSET;
    } else {
        free_offset = page_off + LEAFPG_CELLSOFFSET_OFFSET;
    }

    uint8_t arr2[2];
    memcpy(&mem_page->data[page_off + PGHEADER_PGTYPE_OFFSET], &type, sizeof(uint8_t));

    arr2[0] = (free_offset >> 8) & 0xff;
    arr2[1] = free_offset & 0xff;
    memcpy(&mem_page->data[page_off + PGHEADER_FREE_OFFSET], &arr2, sizeof(uint16_t));
    arr2[0] = (n_cells >> 8) & 0xff;
    arr2[1] = n_cells & 0xff;
    memcpy(&mem_page->data[page_off + PGHEADER_NCELLS_OFFSET], &arr2, sizeof(uint16_t));
    arr2[0] = (cells_offset >> 8) & 0xff;
    arr2[1] = cells_offset & 0xff;
    memcpy(&mem_page->data[page_off + PGHEADER_CELL_OFFSET], &arr2, sizeof(uint16_t));

    if ((ret = chidb_Pager_writePage(bt->pager, mem_page)) != CHIDB_OK) {
        return ret;
    }

    return CHIDB_OK;
}



/* Write an in-memory B-Tree node to disk
 *
 * Writes an in-memory B-Tree node to disk. To do this, we need to update
 * the in-memory page according to the chidb page format. Since the cell
 * offset array and the cells themselves are modified directly on the
 * page, the only thing to do is to store the values of "type",
 * "free_offset", "n_cells", "cells_offset" and "right_page" in the
 * in-memory page.
 *
 * Parameters
 * - bt: B-Tree file
 * - btn: BTreeNode to write to disk
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_writeNode(BTree *bt, BTreeNode *btn)
{
    /* Your code goes here */
    int page_off = 0;
    if (btn->page->npage == 1) {
        page_off = HEADER_OFFSET;
    }
    uint8_t arr2[2];
    uint8_t arr4[4];
    memcpy(&btn->page->data[page_off], &btn->type, sizeof(uint8_t));
    arr2[0] = (btn->free_offset >> 8) & 0xff;
    arr2[1] = btn->free_offset & 0xff;
    memcpy(&btn->page->data[page_off + 1], &arr2, sizeof(uint16_t));
    arr2[0] = (btn->n_cells >> 8) & 0xff;
    arr2[1] = btn->n_cells & 0xff;
    memcpy(&btn->page->data[page_off + 3], &arr2, sizeof(uint16_t));
    arr2[0] = (btn->cells_offset >> 8) & 0xff;
    arr2[1] = btn->cells_offset & 0xff;
    memcpy(&btn->page->data[page_off + 5], &arr2, sizeof(uint16_t));
    if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_INDEX_INTERNAL) {
        arr4[0] = (btn->right_page >> 24) & 0xff;
        arr4[1] = (btn->right_page >> 16) & 0xff;
        arr4[2] = (btn->right_page >> 8) & 0xff;
        arr4[3] = btn->right_page & 0xff;
        memcpy(&btn->page->data[page_off + 8], &arr4, sizeof(uint32_t));
    }
    int ret;
    if ((ret = chidb_Pager_writePage(bt->pager, btn->page)) != CHIDB_OK) {
        return ret;
    }

    return CHIDB_OK;
}


/* Read the contents of a cell
 *
 * Reads the contents of a cell from a BTreeNode and stores them in a BTreeCell.
 * This involves the following:
 *  1. Find out the offset of the requested cell.
 *  2. Read the cell from the in-memory page, and parse its
 *     contents (refer to The chidb File Format document for
 *     the format of cells).
 *
 * Parameters
 * - btn: BTreeNode where cell is contained
 * - ncell: Cell number
 * - cell: BTreeCell where contents must be stored.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_getCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    /* Your code goes here */
    uint16_t off = 0;
    if (btn->page->npage == 1) {
        off += HEADER_OFFSET;
    }
    uint16_t idx_off;
    uint16_t cell_off;
    cell->type = btn->type;
    switch (cell->type) {
    case PGTYPE_TABLE_INTERNAL:
        idx_off = off + INTPG_CELLSOFFSET_OFFSET + ncell * 2;
        cell_off = (btn->page->data[idx_off] << 8) | btn->page->data[idx_off + 1];

        cell->fields.tableInternal.child_page = (btn->page->data[cell_off] << 24) |
                (btn->page->data[cell_off + 1] << 16) |
                (btn->page->data[cell_off + 2] << 8) |
                btn->page->data[cell_off + 3];

        cell->key = (btn->page->data[cell_off + 4] & 0x7f) << 21 |
                (btn->page->data[cell_off + 5] & 0x7f) << 14 |
                (btn->page->data[cell_off + 6] & 0x7f) << 7 |
                (btn->page->data[cell_off + 7] & 0x7f);

        break;
    case PGTYPE_TABLE_LEAF:
        idx_off = off + LEAFPG_CELLSOFFSET_OFFSET + ncell * 2;
        cell_off = (btn->page->data[idx_off] << 8) | btn->page->data[idx_off + 1];

        cell->fields.tableLeaf.data_size = (btn->page->data[cell_off] & 0x7f) << 21 |
                (btn->page->data[cell_off + 1] & 0x7f) << 14 |
                (btn->page->data[cell_off + 2] & 0x7f) << 7 |
                (btn->page->data[cell_off + 3] & 0x7f);

        cell->key = (btn->page->data[cell_off + 4] & 0x7f) << 21 |
                (btn->page->data[cell_off + 5] & 0x7f) << 14 |
                (btn->page->data[cell_off + 6] & 0x7f) << 7 |
                (btn->page->data[cell_off + 7] & 0x7f);

        cell->fields.tableLeaf.data = &btn->page->data[cell_off + 8];

        break;
    case PGTYPE_INDEX_INTERNAL:
        idx_off = off + INTPG_CELLSOFFSET_OFFSET + ncell * 2;
        cell_off = (btn->page->data[idx_off] << 8) | btn->page->data[idx_off + 1];

        cell->fields.indexInternal.child_page = btn->page->data[cell_off] << 24 |
                btn->page->data[cell_off + 1] << 16 |
                btn->page->data[cell_off + 2] << 8 |
                btn->page->data[cell_off + 3];

        cell->key = btn->page->data[cell_off + 8] << 24 |
                btn->page->data[cell_off + 9] << 16 |
                btn->page->data[cell_off + 10] << 8 |
                btn->page->data[cell_off + 11];

        cell->fields.indexInternal.keyPk = btn->page->data[cell_off + 12] << 24 |
                btn->page->data[cell_off + 13] << 16 |
                btn->page->data[cell_off + 14] << 8 |
                btn->page->data[cell_off + 15];

        break;
    case PGTYPE_INDEX_LEAF:
        idx_off = off + LEAFPG_CELLSOFFSET_OFFSET + ncell * 2;
        cell_off = (btn->page->data[idx_off] << 8) | btn->page->data[idx_off + 1];

        cell->key = btn->page->data[cell_off + 4] << 24 |
                btn->page->data[cell_off + 5] << 16 |
                btn->page->data[cell_off + 6] << 8 |
                btn->page->data[cell_off + 7];

        cell->fields.indexLeaf.keyPk = btn->page->data[cell_off + 8] << 24 |
                btn->page->data[cell_off + 9] << 16 |
                btn->page->data[cell_off + 10] << 8 |
                btn->page->data[cell_off + 11];

        break;
    }

    return CHIDB_OK;
}


/* Insert a new cell into a B-Tree node
 *
 * Inserts a new cell into a B-Tree node at a specified position ncell.
 * This involves the following:
 *  1. Add the cell at the top of the cell area. This involves "translating"
 *     the BTreeCell into the chidb format (refer to The chidb File Format
 *     document for the format of cells).
 *  2. Modify cells_offset in BTreeNode to reflect the growth in the cell area.
 *  3. Modify the cell offset array so that all values in positions >= ncell
 *     are shifted one position forward in the array. Then, set the value of
 *     position ncell to be the offset of the newly added cell.
 *
 * This function assumes that there is enough space for this cell in this node.
 *
 * Parameters
 * - btn: BTreeNode to insert cell in
 * - ncell: Cell number
 * - cell: BTreeCell to insert.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ECELLNO: The provided cell number is invalid
 */
int chidb_Btree_insertCell(BTreeNode *btn, ncell_t ncell, BTreeCell *cell)
{
    /* Your code goes here */
    if (ncell < 0 || ncell > btn->n_cells) {
        return CHIDB_ECELLNO;
    }

    uint8_t arr2[2];
    uint8_t arr4[4];
    uint16_t cell_off = btn->cells_offset;
    switch (btn->type) {
    case PGTYPE_TABLE_INTERNAL:
        cell_off -= TABLEINTCELL_SIZE;

        arr4[0] = (cell->fields.tableInternal.child_page >> 24) & 0xff;
        arr4[1] = (cell->fields.tableInternal.child_page >> 16) & 0xff;
        arr4[2] = (cell->fields.tableInternal.child_page >> 8) & 0xff;
        arr4[3] = cell->fields.tableInternal.child_page & 0xff;
        memcpy(&btn->page->data[cell_off + TABLEINTCELL_CHILD_OFFSET], &arr4, 4);

        arr4[0] = (cell->key >> 21) | 0x80;
        arr4[1] = (cell->key >> 14) | 0x80;
        arr4[2] = (cell->key >> 7) | 0x80;
        arr4[3] = cell->key & 0x7f;
        memcpy(&btn->page->data[cell_off + TABLEINTCELL_KEY_OFFSET], &arr4, 4);

        break;
    case PGTYPE_TABLE_LEAF:
        cell_off -= TABLELEAFCELL_SIZE_WITHOUTDATA;
        cell_off -= cell->fields.tableLeaf.data_size;

        arr4[0] = (cell->fields.tableLeaf.data_size >> 21) | 0x80;
        arr4[1] = (cell->fields.tableLeaf.data_size >> 14) | 0x80;
        arr4[2] = (cell->fields.tableLeaf.data_size >> 7) | 0x80;
        arr4[3] = cell->fields.tableLeaf.data_size & 0x7f;
        memcpy(&btn->page->data[cell_off + TABLELEAFCELL_SIZE_OFFSET], &arr4, 4);

        arr4[0] = (cell->key >> 21) | 0x80;
        arr4[1] = (cell->key >> 14) | 0x80;
        arr4[2] = (cell->key >> 7) | 0x80;
        arr4[3] = cell->key & 0x7f;
        memcpy(&btn->page->data[cell_off + TABLELEAFCELL_KEY_OFFSET], &arr4, 4);
        // must use data[0]
        memcpy(&btn->page->data[cell_off + TABLELEAFCELL_DATA_OFFSET], &cell->fields.tableLeaf.data[0], cell->fields.tableLeaf.data_size);

        break;
    case PGTYPE_INDEX_INTERNAL:
        cell_off -= INDEXINTCELL_SIZE;

        arr4[0] = (cell->fields.indexInternal.child_page >> 24) & 0xff;
        arr4[1] = (cell->fields.indexInternal.child_page >> 16) & 0xff;
        arr4[2] = (cell->fields.indexInternal.child_page >> 8) & 0xff;
        arr4[3] = cell->fields.indexInternal.child_page & 0xff;
        memcpy(&btn->page->data[cell_off + INDEXINTCELL_CHILD_OFFSET], &arr4, 4);

        arr4[0] = 0x0B;
        arr4[1] = 0x03;
        arr4[2] = 0x04;
        arr4[3] = 0x04;
        memcpy(&btn->page->data[cell_off + INDEXINTCELL_CHILD_OFFSET + 4], &arr4, 4);

        arr4[0] = (cell->key >> 24) & 0xff;
        arr4[1] = (cell->key >> 16) & 0xff;
        arr4[2] = (cell->key >> 8) & 0xff;
        arr4[3] = cell->key & 0xff;
        memcpy(&btn->page->data[cell_off + INDEXINTCELL_KEYIDX_OFFSET], &arr4, 4);

        arr4[0] = (cell->fields.indexInternal.keyPk >> 24) & 0xff;
        arr4[1] = (cell->fields.indexInternal.keyPk >> 16) & 0xff;
        arr4[2] = (cell->fields.indexInternal.keyPk >> 8) & 0xff;
        arr4[3] = cell->fields.indexInternal.keyPk & 0xff;
        memcpy(&btn->page->data[cell_off + INDEXINTCELL_KEYPK_OFFSET], &arr4, 4);

        break;
    case PGTYPE_INDEX_LEAF:
        cell_off -= INDEXLEAFCELL_SIZE;

        arr4[0] = 0x0B;
        arr4[1] = 0x03;
        arr4[2] = 0x04;
        arr4[3] = 0x04;
        memcpy(&btn->page->data[cell_off + INDEXLEAFCELL_SIZE_OFFSET], &arr4, 4);

        arr4[0] = (cell->key >> 24) & 0xff;
        arr4[1] = (cell->key >> 16) & 0xff;
        arr4[2] = (cell->key >> 8) & 0xff;
        arr4[3] = cell->key & 0xff;
        memcpy(&btn->page->data[cell_off + INDEXLEAFCELL_KEYIDX_OFFSET], &arr4, 4);

        arr4[0] = (cell->fields.indexLeaf.keyPk >> 24) & 0xff;
        arr4[1] = (cell->fields.indexLeaf.keyPk >> 16) & 0xff;
        arr4[2] = (cell->fields.indexLeaf.keyPk >> 8) & 0xff;
        arr4[3] = cell->fields.indexLeaf.keyPk & 0xff;
        memcpy(&btn->page->data[cell_off + INDEXLEAFCELL_KEYPK_OFFSET], &arr4, 4);

        break;
    }

    btn->cells_offset = cell_off;

    uint16_t idx_off = btn->free_offset;
    for (int i = btn->n_cells; i > ncell; i--) {
        memcpy(&btn->page->data[idx_off], &btn->page->data[idx_off - 2], 2);
        idx_off -= sizeof(uint16_t);
    }

    arr2[0] = (btn->cells_offset >> 8) & 0xff;
    arr2[1] = btn->cells_offset & 0xff;
    memcpy(&btn->page->data[idx_off], &arr2, 2);
    btn->n_cells++;
    btn->free_offset += 2;

    return CHIDB_OK;
}

/* Find an entry in a table B-Tree
 *
 * Finds the data associated for a given key in a table B-Tree
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want search in
 * - key: Entry key
 * - data: Out-parameter where a copy of the data must be stored
 * - size: Out-parameter where the number of bytes of data must be stored
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOTFOUND: No entry with the given key way found
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_find(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t **data, uint16_t *size)
{
    /* Your code goes here */
    int ret;
    BTreeNode *btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, nroot, &btn)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell cell;
    for (ncell_t i = 0; i < btn->n_cells; i++) {
        if ((ret = chidb_Btree_getCell(btn, i, &cell)) != CHIDB_OK) {
            return ret;
        }
        switch (cell.type) {
        case PGTYPE_TABLE_INTERNAL:
            if (key <= cell.key) {
                return chidb_Btree_find(bt, cell.fields.tableInternal.child_page, key, data, size);
            }
            break;
        case PGTYPE_TABLE_LEAF:
            if (key == cell.key) {
                *size = cell.fields.tableLeaf.data_size;
                *data = malloc(cell.fields.tableLeaf.data_size);
                if (*data == NULL) {
                    return CHIDB_ENOMEM;
                }
                memcpy(*data, &cell.fields.tableLeaf.data[0], cell.fields.tableLeaf.data_size);
                return CHIDB_OK;
            } else if (key < cell.key) {
                return CHIDB_ENOTFOUND;
            }
            break;
        case PGTYPE_INDEX_INTERNAL:

            break;
        case PGTYPE_INDEX_LEAF:

            break;
        }
    }
    if (btn->type == PGTYPE_TABLE_INTERNAL) {
        if (btn->right_page > 1) {
            return chidb_Btree_find(bt, btn->right_page, key, data, size);
        }
    }

    return CHIDB_ENOTFOUND;
}



/* Insert an entry into a table B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a key and data, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - key: Entry key
 * - data: Pointer to data we want to insert
 * - size: Number of bytes of data
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInTable(BTree *bt, npage_t nroot, chidb_key_t key, uint8_t *data, uint16_t size)
{
    /* Your code goes here */
    BTreeCell btc;
    btc.type = PGTYPE_TABLE_LEAF;
    btc.key = key;
    btc.fields.tableLeaf.data_size = size;
    btc.fields.tableLeaf.data = data;

    return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert an entry into an index B-Tree
 *
 * This is a convenience function that wraps around chidb_Btree_insert.
 * It takes a KeyIdx and a KeyPk, and creates a BTreeCell that can be passed
 * along to chidb_Btree_insert.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this entry in.
 * - keyIdx: See The chidb File Format.
 * - keyPk: See The chidb File Format.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertInIndex(BTree *bt, npage_t nroot, chidb_key_t keyIdx, chidb_key_t keyPk)
{
    /* Your code goes here */
    BTreeCell btc;
    btc.type = PGTYPE_INDEX_LEAF;
    btc.key = keyIdx;
    btc.fields.indexLeaf.keyPk = keyPk;

    return chidb_Btree_insert(bt, nroot, &btc);
}


/* Insert a BTreeCell into a B-Tree
 *
 * The chidb_Btree_insert and chidb_Btree_insertNonFull functions
 * are responsible for inserting new entries into a B-Tree, although
 * chidb_Btree_insertNonFull is the one that actually does the
 * insertion. chidb_Btree_insert, however, first checks if the root
 * has to be split (a splitting operation that is different from
 * splitting any other node). If so, chidb_Btree_split is called
 * before calling chidb_Btree_insertNonFull.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insert(BTree *bt, npage_t nroot, BTreeCell *btc)
{
    /* Your code goes here */
    int ret;
    BTreeNode *btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, nroot, &btn)) != CHIDB_OK) {
        return ret;
    }

    uint8_t is_root_full = 0;
    uint16_t free_space = btn->cells_offset - btn->free_offset;
    switch (btn->type) {
    case PGTYPE_TABLE_INTERNAL:
        if (free_space < TABLEINTCELL_SIZE + 2) {
            is_root_full = 1;
        }
        break;
    case PGTYPE_TABLE_LEAF:
        if (free_space < TABLELEAFCELL_SIZE_WITHOUTDATA + btc->fields.tableLeaf.data_size + 2) {
            is_root_full = 1;
        }
        break;
    case PGTYPE_INDEX_INTERNAL:
        if (free_space < INDEXINTCELL_SIZE + 2) {
            is_root_full = 1;
        }
        break;
    case PGTYPE_INDEX_LEAF:
        if (free_space < INDEXLEAFCELL_SIZE + 2) {
            is_root_full = 1;
        }
        break;
    }
    if (is_root_full == 1) {
        BTreeCell root_btc;
        ncell_t mid_cell = (btn->n_cells - 1) / 2;
        // write left child node
        npage_t left_page;
        BTreeNode *left_btn;
        if ((ret = chidb_Btree_newNode(bt, &left_page, btn->type)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Btree_getNodeByPage(bt, left_page, &left_btn)) != CHIDB_OK) {
            return ret;
        }
        // insert left cell
        ncell_t left_mid_cell = mid_cell;
        if (btn->type == PGTYPE_INDEX_INTERNAL || btn->type == PGTYPE_INDEX_LEAF) {
            left_mid_cell--;
        }
        for (ncell_t i = 0; i <= left_mid_cell; i++) {
            if ((ret = chidb_Btree_getCell(btn, i, &root_btc)) != CHIDB_OK) {
                return ret;
            }
            if ((ret = chidb_Btree_insertCell(left_btn, i, &root_btc)) != CHIDB_OK) {
                return ret;
            }
        }
        if ((ret = chidb_Btree_writeNode(bt, left_btn)) != CHIDB_OK) {
            return ret;
        }

        // write right child node
        npage_t right_page;
        BTreeNode *right_btn;
        if ((ret = chidb_Btree_newNode(bt, &right_page, btn->type)) != CHIDB_OK) {
           return ret;
        }
        if ((ret = chidb_Btree_getNodeByPage(bt, right_page, &right_btn)) != CHIDB_OK) {
            return ret;
        }
        int j = 0;
        for (ncell_t i = mid_cell + 1; i < btn->n_cells; i++) {
            if ((ret = chidb_Btree_getCell(btn, i, &root_btc)) != CHIDB_OK) {
                return ret;
            }
            if ((ret = chidb_Btree_insertCell(right_btn, j, &root_btc)) != CHIDB_OK) {
                return ret;
            }
            j++;
        }
        right_btn->right_page = btn->right_page;
        if ((ret = chidb_Btree_writeNode(bt, right_btn)) != CHIDB_OK) {
            return ret;
        }

        // write root node
        if ((ret = chidb_Btree_getCell(btn, mid_cell, &root_btc)) != CHIDB_OK) {
            return ret;
        }

        uint8_t new_type = 0;
        if (btn->type == PGTYPE_TABLE_INTERNAL || btn->type == PGTYPE_TABLE_LEAF) {
            new_type = PGTYPE_TABLE_INTERNAL;
        } else if (btn->type == PGTYPE_INDEX_INTERNAL || btn->type == PGTYPE_INDEX_LEAF) {
            new_type = PGTYPE_INDEX_INTERNAL;
        }
        if ((ret = chidb_Btree_initEmptyNode(bt, nroot, new_type)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Btree_getNodeByPage(bt, nroot, &btn)) != CHIDB_OK) {
            return ret;
        }
        root_btc.type = new_type;
        switch (new_type) {
        case PGTYPE_TABLE_INTERNAL:
            root_btc.fields.tableInternal.child_page = left_page;
            break;
        case PGTYPE_INDEX_INTERNAL:
            root_btc.fields.indexInternal.child_page = left_page;
            break;
        }
        if ((ret = chidb_Btree_insertCell(btn, 0, &root_btc)) != CHIDB_OK) {
            return ret;
        }
        btn->right_page = right_page;
        if ((ret = chidb_Btree_writeNode(bt, btn)) != CHIDB_OK) {
            return ret;
        }
    }

    return chidb_Btree_insertNonFull(bt, nroot, btc);
}

/* Insert a BTreeCell into a non-full B-Tree node
 *
 * chidb_Btree_insertNonFull inserts a BTreeCell into a node that is
 * assumed not to be full (i.e., does not require splitting). If the
 * node is a leaf node, the cell is directly added in the appropriate
 * position according to its key. If the node is an internal node, the
 * function will determine what child node it must insert it in, and
 * calls itself recursively on that child node. However, before doing so
 * it will check if the child node is full or not. If it is, then it will
 * have to be split first.
 *
 * Parameters
 * - bt: B-Tree file
 * - nroot: Page number of the root node of the B-Tree we want to insert
 *          this cell in.
 * - btc: BTreeCell to insert into B-Tree
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_EDUPLICATE: An entry with that key already exists
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_insertNonFull(BTree *bt, npage_t npage, BTreeCell *btc)
{
    /* Your code goes here */
    int ret;
    BTreeNode *btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, npage, &btn)) != CHIDB_OK) {
        return ret;
    }
    // if cell in leaf node, insert directly
    if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF) {
        ncell_t insert_cell = btn->n_cells;
        BTreeCell orig_btc;
        for (ncell_t i = 0; i < btn->n_cells; i++) {
            if ((ret = chidb_Btree_getCell(btn, i, &orig_btc)) != CHIDB_OK) {
                return ret;
            }
            if (btc->key == orig_btc.key) {
                return CHIDB_EDUPLICATE;
            } else if (btc->key < orig_btc.key) {
                insert_cell = i;
                break;
            }
        }

        if ((ret = chidb_Btree_insertCell(btn, insert_cell, btc)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Btree_writeNode(bt, btn)) != CHIDB_OK) {
            return ret;
        }

        return CHIDB_OK;
    }

    // if cell in internal node
    // 1. find child node, if no exist, create one
    // 2. check child node is full, if so split it
    // 3. insert cell to child node
    ncell_t parent_cell = btn->n_cells;
    npage_t child_page = btn->right_page;
    BTreeCell search_btc;
    uint8_t find_child = 0;
    for (ncell_t i = 0; i < btn->n_cells; i++) {
        if ((ret = chidb_Btree_getCell(btn, i, &search_btc)) != CHIDB_OK) {
            return ret;
        }
        switch (btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            if (btc->key <= search_btc.key) {
                parent_cell = i;
                child_page = search_btc.fields.tableInternal.child_page;
                find_child = 1;
            }
            break;
        case PGTYPE_INDEX_INTERNAL:
            if (btc->key == search_btc.key) {
                return CHIDB_EDUPLICATE;
            } else if (btc->key < search_btc.key) {
                parent_cell = i;
                child_page = search_btc.fields.indexInternal.child_page;
                find_child = 1;
            }
            break;
        }
        if (find_child == 1) {
            break;
        }
    }

    BTreeNode *child_btn;
    if (child_page > 0) {
        // check child is full
        uint8_t is_full = 0;
        uint16_t free_space = 0;
        if ((ret = chidb_Btree_getNodeByPage(bt, child_page, &child_btn)) != CHIDB_OK) {
            return ret;
        }
        free_space = child_btn->cells_offset - child_btn->free_offset;
        switch (child_btn->type) {
        case PGTYPE_TABLE_INTERNAL:
            if (free_space < TABLEINTCELL_SIZE + 2) {
                is_full = 1;
            }
            break;
        case PGTYPE_TABLE_LEAF:
            if (free_space < TABLELEAFCELL_SIZE_WITHOUTDATA + btc->fields.tableLeaf.data_size + 2) {
                is_full = 1;
            }
            break;
        case PGTYPE_INDEX_INTERNAL:
            if (free_space < INDEXINTCELL_SIZE + 2) {
                is_full = 1;
            }
            break;
        case PGTYPE_INDEX_LEAF:
            if (free_space < INDEXLEAFCELL_SIZE + 2) {
                is_full = 1;
            }
            break;
        }
        // split page
        if (is_full == 1) {
            npage_t child_page2 = 0;
            if ((ret = chidb_Btree_split(bt, npage, child_page, parent_cell, &child_page2)) != CHIDB_OK) {
                return ret;
            }
            if ((ret = chidb_Btree_getNodeByPage(bt, npage, &btn)) != CHIDB_OK) {
                return ret;
            }
            if ((ret = chidb_Btree_getCell(btn, parent_cell, &search_btc)) != CHIDB_OK) {
                return ret;
            }
            if (btc->key <= search_btc.key) {
                child_page = child_page2;
            }
        }
    } else {
        // create a child page
        uint8_t child_type = 0;
        if (btn->type == PGTYPE_TABLE_INTERNAL) {
            child_type = PGTYPE_TABLE_LEAF;
        } else if (btn->type == PGTYPE_INDEX_INTERNAL) {
            child_type = PGTYPE_INDEX_LEAF;
        }
        if ((ret = chidb_Btree_newNode(bt, &child_page, child_type)) != CHIDB_OK) {
            return ret;
        }
        if (btn->n_cells != 0) {
            btn->right_page = child_page;
        } else {
            BTreeCell parent_btc;
            parent_btc.type = btn->type;
            parent_btc.key = btc->key;

            switch (btn->type) {
            case PGTYPE_TABLE_INTERNAL:
                parent_btc.fields.tableInternal.child_page = child_page;
                break;
            case PGTYPE_INDEX_INTERNAL:
                parent_btc.fields.indexInternal.child_page = child_page;
                break;
            }
            if ((ret = chidb_Btree_insertCell(btn, 0, &parent_btc)) != CHIDB_OK) {
                return ret;
            }
        }
        if ((ret = chidb_Btree_writeNode(bt, btn)) != CHIDB_OK) {
            return ret;
        }
        if (btn->type == PGTYPE_INDEX_INTERNAL) {
            return CHIDB_OK;
        }
    }

    return chidb_Btree_insertNonFull(bt, child_page, btc);
}


/* Split a B-Tree node
 *
 * Splits a B-Tree node N. This involves the following:
 * - Find the median cell in N.
 * - Create a new B-Tree node M.
 * - Move the cells before the median cell to M (if the
 *   cell is a table leaf cell, the median cell is moved too)
 * - Add a cell to the parent (which, by definition, will be an
 *   internal page) with the median key and the page number of M.
 *
 * Parameters
 * - bt: B-Tree file
 * - npage_parent: Page number of the parent node
 * - npage_child: Page number of the node to split
 * - parent_ncell: Position in the parent where the new cell will
 *                 be inserted.
 * - npage_child2: Out parameter. Used to return the page of the new child node.
 *
 * Return
 * - CHIDB_OK: Operation successful
 * - CHIDB_ENOMEM: Could not allocate memory
 * - CHIDB_EIO: An I/O error has occurred when accessing the file
 */
int chidb_Btree_split(BTree *bt, npage_t npage_parent, npage_t npage_child, ncell_t parent_ncell, npage_t *npage_child2)
{
    /* Your code goes here */
    int ret;
    BTreeNode *parent_btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, npage_parent, &parent_btn)) != CHIDB_OK) {
        return ret;
    }
    BTreeNode *orig_btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, npage_child, &orig_btn)) != CHIDB_OK) {
        return ret;
    }

    // new node at left
    npage_t left_page;
    if ((ret = chidb_Btree_newNode(bt, &left_page, orig_btn->type)) != CHIDB_OK) {
        return ret;
    }
    BTreeNode *left_btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, left_page, &left_btn)) != CHIDB_OK) {
        return ret;
    }
    // old node at right
    if ((ret = chidb_Btree_initEmptyNode(bt, npage_child, orig_btn->type)) != CHIDB_OK) {
        return ret;
    }
    BTreeNode *right_btn;
    if ((ret = chidb_Btree_getNodeByPage(bt, npage_child, &right_btn)) != CHIDB_OK) {
        return ret;
    }

    BTreeCell orig_btc;
    ncell_t mid_cell = (orig_btn->n_cells - 1) / 2;
    // insert left cell
    ncell_t left_mid_cell = mid_cell;
    if (parent_btn->type == PGTYPE_INDEX_INTERNAL || parent_btn->type == PGTYPE_INDEX_LEAF) {
        left_mid_cell--;
    }
    for (ncell_t i = 0; i <= left_mid_cell; i++) {
        if ((ret = chidb_Btree_getCell(orig_btn, i, &orig_btc)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Btree_insertCell(left_btn, i, &orig_btc)) != CHIDB_OK) {
            return ret;
        }
    }
    if ((ret = chidb_Btree_writeNode(bt, left_btn)) != CHIDB_OK) {
        return ret;
    }

    // insert right cell
    ncell_t j = 0;
    for (ncell_t i = mid_cell + 1; i < orig_btn->n_cells; i++) {
        if ((ret = chidb_Btree_getCell(orig_btn, i, &orig_btc)) != CHIDB_OK) {
            return ret;
        }
        if ((ret = chidb_Btree_insertCell(right_btn, j, &orig_btc)) != CHIDB_OK) {
            return ret;
        }
        j++;
    }
    right_btn->right_page = orig_btn->right_page;
    if ((ret = chidb_Btree_writeNode(bt, right_btn)) != CHIDB_OK) {
        return ret;
    }

    // insert parent cell
    BTreeCell new_btc;
    if ((ret = chidb_Btree_getCell(orig_btn, mid_cell, &orig_btc)) != CHIDB_OK) {
        return ret;
    }
    new_btc.type = parent_btn->type;
    new_btc.key = orig_btc.key;
    switch(new_btc.type) {
    case PGTYPE_TABLE_INTERNAL:
        new_btc.fields.tableInternal.child_page = left_page;
        break;
    case PGTYPE_INDEX_INTERNAL:
        new_btc.fields.indexInternal.child_page = left_page;
        new_btc.fields.indexInternal.keyPk = orig_btc.fields.indexInternal.keyPk;
        break;
    }
    if ((ret = chidb_Btree_insertCell(parent_btn, parent_ncell, &new_btc)) != CHIDB_OK) {
        return ret;
    }
    if ((ret = chidb_Btree_writeNode(bt, parent_btn)) != CHIDB_OK) {
        return ret;
    }
    *npage_child2 = left_page;

    return CHIDB_OK;
}

