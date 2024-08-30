## root_id

This is the id of the root node. It is valid at all times. It is initially a `LEAF` node. It is updated only
by `create_new_root` where it becomes an `INTERNAL` node.

## head_id

This is the id of the head node. It is valid at all times. It is always a `LEAF` node. It is never updated because there
is no delete operation. Any `LEAF` that has a different id is guaranteed to have a previous node. If the fast path does
not point to this node, `fp_min` is the lower bound of the fast node.

## tail_id

This is the id of the tail node. It is valid at all times. It is always a `LEAF` node. It is updated whenever the tail
splits. Any `LEAF` that has a different id is guaranteed to have a next node. If the fast path does not point to this
node, `fp_max` is the upper bound of the fast node.

# FAST PATH

## fp_id

This is the id of the fast node (tail/lil/lol). It is valid at all times. It is always a `LEAF` node. It is updated
according to the update policy of the strategy.

- tail: it is always equal to the `tail_id`.
- lil: it is updated to the node id that got the latest insert.
- lol: read the paper.

## fp_min

This is the minimum value that the fast node accepts (inclusive). It is valid only when `fp_id` is not equal
to `head_id`.

## fp_max

This is the maximum value that the fast node accepts (exclusive). It is valid only when `fp_id` is not equal
to `tail_id`.

## lol_prev_id

This is the id of the previous node of the fast node. It is invalid after a reset.
