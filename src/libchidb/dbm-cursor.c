/*
 *  chidb - a didactic relational database management system
 *
 *  Database Machine cursors
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


#include "dbm-cursor.h"

/* Your code goes here */

int chidb_cursor_open(chidb_dbm_cursor_type_t type, npage_t nroot, int32_t col_num, chidb_dbm_cursor_t *cursor) {
    cursor->type = type;
    cursor->nroot = nroot;
    cursor->col_num = col_num;

    return CHIDB_OK;
}

int chidb_cursor_close(BTree *bt, chidb_dbm_cursor_t *cursor) {
    int ret;
    while(1) {
        if (cursor->node_list == NULL) {
            break;
        }
        if ((ret = chidb_Btree_freeMemNode(bt, cursor->node_list->btn)) != CHIDB_OK) {
            return ret;
        }
        chidb_dbm_cursor_node_list_t *pcnl = cursor->node_list->parent;
        free(cursor->node_list);
        cursor->node_list = pcnl;
    }

    return CHIDB_OK;
}

int chidb_cursor_rewind(BTree *bt, chidb_dbm_cursor_t *cursor) {
    int ret;
    chidb_dbm_cursor_node_list_t *pcnl = NULL;
    npage_t npage = cursor->nroot;
    while (1) {
        BTreeNode *btn;
        if ((ret = chidb_Btree_getNodeByPage(bt, npage, &btn)) != CHIDB_OK) {
            return ret;
        }
        if (btn->n_cells == 0) {
            return CHIDB_EEMPTY;
        }
        chidb_dbm_cursor_node_list_t *cnl = malloc(sizeof(chidb_dbm_cursor_node_list_t));
        if (cnl == NULL) {
            return CHIDB_ENOMEM;
        }
        cnl->npage = npage;
        cnl->ncell = 0;
        cnl->is_right = 0;
        cnl->btn = btn;
        cnl->parent = pcnl;
        pcnl = cnl;
        cursor->node_list = cnl;
        if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF) {
            break;
        }

        BTreeCell btc;
        if ((ret = chidb_Btree_getCell(btn, 0, &btc)) != CHIDB_OK) {
            return ret;
        }
        switch (btc.type) {
        case PGTYPE_TABLE_INTERNAL:
            npage = btc.fields.tableInternal.child_page;
            break;
        case PGTYPE_TABLE_LEAF:

            break;
        case PGTYPE_INDEX_INTERNAL:
            npage = btc.fields.indexInternal.child_page;
            break;
        case PGTYPE_INDEX_LEAF:

            break;
        }
    }

    return CHIDB_OK;
}


int chidb_cursor_next(BTree *bt, chidb_dbm_cursor_t *cursor) {
    int ret;
    chidb_dbm_cursor_node_list_t *cnl = cursor->node_list;
    if (cnl == NULL) {
        return CHIDB_EEMPTY;
    }
    if (cnl->ncell < cnl->btn->n_cells - 1) {
        cnl->ncell++;
        return CHIDB_OK;
    }

    int has_child = 0;
    BTreeCell btc;
    npage_t npage = 0;
    chidb_dbm_cursor_node_list_t *pcnl = NULL;
    while (1) {
        cnl = cnl->parent;
        pcnl = cnl;
        cursor->node_list = cnl;
        if (cnl == NULL) {
            return CHIDB_EEMPTY;
        }
        if (cnl->is_right == 1) {
            continue;
        }
        if (cnl->ncell < cnl->btn->n_cells - 1) {
            cnl->ncell++;
            if ((ret = chidb_Btree_getCell(cnl->btn, cnl->ncell, &btc)) != CHIDB_OK) {
                return ret;
            }
            switch (btc.type) {
            case PGTYPE_TABLE_INTERNAL:
                npage = btc.fields.tableInternal.child_page;
                has_child = 1;
                break;
            case PGTYPE_TABLE_LEAF:
                break;
            case PGTYPE_INDEX_INTERNAL:
                npage = btc.fields.indexInternal.child_page;
                has_child = 1;
                break;
            case PGTYPE_INDEX_LEAF:
                break;
            }
            break;
        }
        if (cnl->btn->right_page != 0) {
            npage = cnl->btn->right_page;
            cnl->ncell = cnl->btn->n_cells;
            cnl->is_right = 1;
            has_child = 1;
            break;
        }
    }
    if (has_child == 0) {
        return CHIDB_EEMPTY;
    }

    while (1) {
        BTreeNode *btn;
        if ((ret = chidb_Btree_getNodeByPage(bt, npage, &btn)) != CHIDB_OK) {
            return ret;
        }
        if (btn->n_cells == 0) {
            return CHIDB_EEMPTY;
        }
        chidb_dbm_cursor_node_list_t *cnl = malloc(sizeof(chidb_dbm_cursor_node_list_t));
        if (cnl == NULL) {
            return CHIDB_ENOMEM;
        }
        cnl->npage = npage;
        cnl->ncell = 0;
        cnl->btn = btn;
        cnl->parent = pcnl;
        pcnl = cnl;
        cursor->node_list = cnl;
        if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF) {
            break;
        }

        BTreeCell btc;
        if ((ret = chidb_Btree_getCell(btn, 0, &btc)) != CHIDB_OK) {
            return ret;
        }
        switch (btc.type) {
        case PGTYPE_TABLE_INTERNAL:
            npage = btc.fields.tableInternal.child_page;
            break;
        case PGTYPE_TABLE_LEAF:

            break;
        case PGTYPE_INDEX_INTERNAL:
            npage = btc.fields.indexInternal.child_page;
            break;
        case PGTYPE_INDEX_LEAF:

            break;
        }
    }

    return CHIDB_OK;
}

int chidb_cursor_prev(BTree *bt, chidb_dbm_cursor_t *cursor) {
    int ret;
    chidb_dbm_cursor_node_list_t *cnl = cursor->node_list;
    if (cnl == NULL) {
        return CHIDB_EEMPTY;
    }
    if (cnl->ncell > 0) {
        cnl->ncell--;
        return CHIDB_OK;
    }

    BTreeCell btc;
    npage_t npage = 0;
    chidb_dbm_cursor_node_list_t *pcnl = NULL;
    while (1) {
        cnl = cnl->parent;
        pcnl = cnl;
        cursor->node_list = cnl;
        if (cnl == NULL) {
            return CHIDB_EEMPTY;
        }
        if (cnl->ncell > 0) {
            cnl->ncell--;
            if ((ret = chidb_Btree_getCell(cnl->btn, cnl->ncell, &btc)) != CHIDB_OK) {
                return ret;
            }
            switch (btc.type) {
            case PGTYPE_TABLE_INTERNAL:
                npage = btc.fields.tableInternal.child_page;
                break;
            case PGTYPE_TABLE_LEAF:
                break;
            case PGTYPE_INDEX_INTERNAL:
                npage = btc.fields.indexInternal.child_page;
                break;
            case PGTYPE_INDEX_LEAF:
                break;
            }
            break;
        }
    }

    while (1) {
        BTreeNode *btn;
        if ((ret = chidb_Btree_getNodeByPage(bt, npage, &btn)) != CHIDB_OK) {
            return ret;
        }
        if (btn->n_cells == 0) {
            return CHIDB_EEMPTY;
        }
        chidb_dbm_cursor_node_list_t *cnl = malloc(sizeof(chidb_dbm_cursor_node_list_t));
        if (cnl == NULL) {
            return CHIDB_ENOMEM;
        }
        cnl->npage = npage;
        cnl->ncell = btn->n_cells - 1;
        cnl->is_right = 0;
        cnl->btn = btn;
        cnl->parent = pcnl;
        pcnl = cnl;
        cursor->node_list = cnl;
        if (btn->type == PGTYPE_TABLE_LEAF || btn->type == PGTYPE_INDEX_LEAF) {
            break;
        }
        if (btn->right_page > 0) {
            cnl->ncell = btn->n_cells;
            cnl->is_right = 1;
            npage = btn->right_page;
            continue;
        }

        BTreeCell btc;
        if ((ret = chidb_Btree_getCell(btn, btn->n_cells - 1, &btc)) != CHIDB_OK) {
            return ret;
        }
        switch (btc.type) {
        case PGTYPE_TABLE_INTERNAL:
            npage = btc.fields.tableInternal.child_page;
            break;
        case PGTYPE_TABLE_LEAF:

            break;
        case PGTYPE_INDEX_INTERNAL:
            npage = btc.fields.indexInternal.child_page;
            break;
        case PGTYPE_INDEX_LEAF:

            break;
        }
    }

    return CHIDB_OK;
}

int chidb_cursor_seek(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key) {
    int ret;
    if ((ret = chidb_cursor_rewind(bt, cursor)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell btc;
    while (1) {
        if ((ret = chidb_Btree_getCell(cursor->node_list->btn, cursor->node_list->ncell, &btc)) != CHIDB_OK) {
            break;
        }
        if (key == btc.key) {
            return CHIDB_OK;
        }
        if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
            break;
        }
    }
    return ret;
}

int chidb_cursor_seek_gt(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key) {
    int ret;
    if ((ret = chidb_cursor_rewind(bt, cursor)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell btc;
    while (1) {
        if ((ret = chidb_Btree_getCell(cursor->node_list->btn, cursor->node_list->ncell, &btc)) != CHIDB_OK) {
            break;
        }
        if (key == btc.key) {
            if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
                break;
            }
            return CHIDB_OK;
        } else if (key < btc.key) {
            return CHIDB_OK;
        }
        if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
            break;
        }
    }
    return ret;
}

int chidb_cursor_seek_ge(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key) {
    int ret;
    if ((ret = chidb_cursor_rewind(bt, cursor)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell btc;
    while (1) {
        if ((ret = chidb_Btree_getCell(cursor->node_list->btn, cursor->node_list->ncell, &btc)) != CHIDB_OK) {
            break;
        }
        if (key == btc.key) {
            return CHIDB_OK;
        } else if (key < btc.key) {
            chidb_cursor_prev(bt, cursor);
            break;
        }
        if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
            break;
        }
    }
    return ret;
}

int chidb_cursor_seek_lt(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key) {
    int ret;
    if ((ret = chidb_cursor_rewind(bt, cursor)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell btc;
    while (1) {
        if ((ret = chidb_Btree_getCell(cursor->node_list->btn, cursor->node_list->ncell, &btc)) != CHIDB_OK) {
            break;
        }
        if (key == btc.key) {
            if ((ret = chidb_cursor_prev(bt, cursor)) != CHIDB_OK) {
                break;
            }
            return CHIDB_OK;
        } else if (key < btc.key) {
            return CHIDB_OK;
        }
        if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
            break;
        }
    }
    return ret;
}

int chidb_cursor_seek_le(BTree *bt, chidb_dbm_cursor_t *cursor, chidb_key_t key) {
    int ret;
    if ((ret = chidb_cursor_rewind(bt, cursor)) != CHIDB_OK) {
        return ret;
    }
    BTreeCell btc;
    while (1) {
        if ((ret = chidb_Btree_getCell(cursor->node_list->btn, cursor->node_list->ncell, &btc)) != CHIDB_OK) {
            break;
        }
        if (key <= btc.key) {
            return CHIDB_OK;
        }
        if ((ret = chidb_cursor_next(bt, cursor)) != CHIDB_OK) {
            break;
        }
    }
    return ret;
}

