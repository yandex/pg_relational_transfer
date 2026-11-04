from io import StringIO
from logging import getLogger
import re

from src.data_structures.table_tree import TableTree, TableTreeEdge


logger = getLogger("TABLE_TREE_LOADER")


class TableTreeLoader:
    @classmethod
    def _convert_str_key_to_tuple(cls, str_key: str) -> tuple:
        return tuple(map(lambda k: k.strip(" '\""), filter(lambda k: k, str_key.split(","))))

    @classmethod
    def _create_edge_from_line(cls, source_node: TableTree, line: str) -> TableTreeEdge:
        line_match = re.match(
            r"(^\((?P<source_key>.+)\)\s*->\s*(?P<target_table>\w+).\((?P<target_key>.+)\)(\s+\"(?P<where_clause>.+)\")?$)|(^(?P<init_table>\w+)(\s+\"(?P<init_where_clause>.+)\")?$)",
            line.strip(),
        )
        if not line_match:
            raise ValueError(f"Invalid line: {line}")
        match_dict = line_match.groupdict()

        if match_dict["source_key"] is None:
            return TableTreeEdge(  # imaginary edge
                source_node=source_node,
                target_node=TableTree(line_match["init_table"]),
                source_key=(),
                target_key=(),
                where_clause=match_dict["init_where_clause"],
            )
        source_key = cls._convert_str_key_to_tuple(match_dict["source_key"])
        target_key = cls._convert_str_key_to_tuple(match_dict["target_key"])
        return TableTreeEdge(
            source_node=source_node,
            target_node=TableTree(line_match["target_table"]),
            source_key=source_key,
            target_key=target_key,
            where_clause=match_dict["where_clause"],
        )

    @classmethod
    def _build_table_tree_from_file(cls, file: StringIO) -> TableTree:
        logger.debug("Building table tree from file")
        table_tree_root = TableTree[str, str](None)  # imaginary root
        parent_stack: list[TableTree] = [table_tree_root]
        indent_size = -1
        for line in file:
            if not line.strip():
                continue
            indentation = len(line) - len(line.lstrip())
            if indentation > 0 and indent_size == -1:
                indent_size = indentation
            current_level = (indentation // indent_size if indent_size != -1 else 0) + 1
            while len(parent_stack) > current_level:
                parent_stack.pop()
            parent = parent_stack[-1]
            new_edge = cls._create_edge_from_line(source_node=parent_stack[-1], line=line)
            parent.edges.append(new_edge)
            parent_stack.append(new_edge.target_node)
            logger.debug(
                'New edge: %s.%s -> %s.%s "%s"',
                new_edge.source_table,
                new_edge.source_key,
                new_edge.target_table,
                new_edge.target_key,
                new_edge.where_clause,
            )
        return table_tree_root

    @classmethod
    def load_table_tree(cls, filename: str | None) -> TableTree:
        if not filename:
            return TableTree[str, str](None)
        with open(filename) as file:
            table_tree = cls._build_table_tree_from_file(file=file)
        return table_tree
