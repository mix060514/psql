"""
測試 PostgreSQL Schema 功能的測試文件
"""
import pandas as pd
import pytest
from psql.pg import PG


class TestSchemaFunctionality:
    """測試 Schema 相關功能"""
    
    @pytest.fixture
    def pg_instance(self):
        """創建 PG 實例"""
        return PG()
    
    @pytest.fixture
    def sample_dataframe(self):
        """創建測試用的 DataFrame"""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['張三', '李四', '王五', '趙六', '錢七'],
            'age': [25, 30, 35, 28, 33],
            'salary': [50000.5, 60000.0, 55000.75, 52000.25, 58000.0],
            'is_active': [True, False, True, True, False],
            'join_date': pd.to_datetime(['2023-01-15', '2022-06-20', '2023-03-10', '2022-11-05', '2023-02-28'])
        })
    
    def test_schema_creation_and_listing(self, pg_instance):
        """測試 schema 創建和列表功能"""
        test_schema = 'test_hr_department'
        
        # 清理可能存在的測試 schema
        try:
            pg_instance.drop_schema(test_schema, cascade=True)
        except:
            pass
        
        # 測試創建 schema
        pg_instance.create_schema(test_schema)
        
        # 驗證 schema 是否存在
        assert pg_instance.schema_exists(test_schema), f"Schema {test_schema} 應該已經被創建"
        
        # 測試列出 schemas
        schemas = pg_instance.list_schemas()
        assert test_schema in schemas['schema_name'].values, f"Schema {test_schema} 應該出現在 schema 列表中"
        
        # 清理
        pg_instance.drop_schema(test_schema, cascade=True)
        assert not pg_instance.schema_exists(test_schema), f"Schema {test_schema} 應該已經被刪除"
    
    def test_table_name_parsing(self, pg_instance):
        """測試表格名稱解析功能"""
        # 測試只有表格名稱的情況
        schema, table = pg_instance._parse_table_name('employees')
        assert schema == 'public', "預設 schema 應該是 public"
        assert table == 'employees', "表格名稱應該是 employees"
        
        # 測試包含 schema 的情況
        schema, table = pg_instance._parse_table_name('hr.employees')
        assert schema == 'hr', "Schema 應該是 hr"
        assert table == 'employees', "表格名稱應該是 employees"
        
        # 測試帶引號的情況
        schema, table = pg_instance._parse_table_name('"test_schema"."test_table"')
        assert schema == 'test_schema', "Schema 應該是 test_schema"
        assert table == 'test_table', "表格名稱應該是 test_table"
    
    def test_dataframe_insertion_with_schema(self, pg_instance, sample_dataframe):
        """測試帶 schema 的 DataFrame 插入功能"""
        test_schema = 'test_company'
        table_name = f'{test_schema}.employees'
        
        # 清理可能存在的測試資料
        try:
            pg_instance.drop_schema(test_schema, cascade=True)
        except:
            pass
        
        # 插入 DataFrame 到指定 schema
        pg_instance.insert_pg(sample_dataframe, table_name, overwrite=True)
        
        # 驗證 schema 是否被自動創建
        assert pg_instance.schema_exists(test_schema), f"Schema {test_schema} 應該被自動創建"
        
        # 驗證表格是否存在
        assert pg_instance.table_exists('employees', test_schema), "employees 表格應該存在"
        
        # 查詢插入的數據
        result = pg_instance.query(f"SELECT * FROM {table_name} ORDER BY id")
        
        # 驗證數據完整性
        assert len(result) == len(sample_dataframe), "插入的數據行數應該相等"
        assert list(result.columns) == list(sample_dataframe.columns), "列名應該相等"
        
        # 驗證數據內容
        for i, row in result.iterrows():
            original_row = sample_dataframe.iloc[i]
            assert row['id'] == original_row['id'], f"ID 在第 {i} 行應該相等"
            assert row['name'] == original_row['name'], f"姓名在第 {i} 行應該相等"
            assert row['age'] == original_row['age'], f"年齡在第 {i} 行應該相等"
        
        # 清理
        pg_instance.drop_schema(test_schema, cascade=True)
    
    def test_table_management_functions(self, pg_instance, sample_dataframe):
        """測試表格管理功能"""
        test_schema = 'test_management'
        
        # 清理
        try:
            pg_instance.drop_schema(test_schema, cascade=True)
        except:
            pass
        
        # 創建 schema 和表格
        pg_instance.insert_pg(sample_dataframe, f'{test_schema}.employees', overwrite=True)
        
        # 測試列出表格
        tables = pg_instance.list_tables(test_schema)
        assert 'employees' in tables['table_name'].values, "employees 表格應該在列表中"
        
        # 測試描述表格
        table_info = pg_instance.describe_table('employees', test_schema)
        expected_columns = ['id', 'name', 'age', 'salary', 'is_active', 'join_date']
        
        assert len(table_info) == len(expected_columns), "列數應該相等"
        for col in expected_columns:
            assert col in table_info['column_name'].values, f"列 {col} 應該存在"
        
        # 測試使用完整表格名稱的描述功能
        table_info2 = pg_instance.describe_table(f'{test_schema}.employees')
        assert len(table_info2) == len(table_info), "兩種方式描述的結果應該相同"
        
        # 清理
        pg_instance.drop_schema(test_schema, cascade=True)
    
    def test_datatype_mapping(self, pg_instance):
        """測試數據類型映射功能"""
        # 創建包含各種數據類型的 DataFrame
        test_df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'bigint_col': [2147483648, 2147483649, 2147483650],  # 超過 INTEGER 範圍
            'float_col': [1.5, 2.7, 3.14],
            'bool_col': [True, False, True],
            'str_col': ['短文本', 'Medium length text', 'Very long text that exceeds normal limits'],
            'datetime_col': pd.to_datetime(['2023-01-01', '2023-06-15', '2023-12-31'])
        })
        
        pg_types = pg_instance._get_pg_types(test_df)
        
        assert pg_types['int_col'] == 'INTEGER', "小整數應該映射為 INTEGER"
        assert pg_types['bigint_col'] == 'BIGINT', "大整數應該映射為 BIGINT"
        assert pg_types['float_col'] == 'DOUBLE PRECISION', "浮點數應該映射為 DOUBLE PRECISION"
        assert pg_types['bool_col'] == 'BOOLEAN', "布爾值應該映射為 BOOLEAN"
        assert pg_types['str_col'] in ['TEXT', 'VARCHAR(255)'], "字符串應該映射為 TEXT 或 VARCHAR"
        assert pg_types['datetime_col'] == 'TIMESTAMP', "日期時間應該映射為 TIMESTAMP"
    
    def test_overwrite_functionality(self, pg_instance, sample_dataframe):
        """測試覆蓋功能"""
        test_schema = 'test_overwrite'
        table_name = f'{test_schema}.test_table'
        
        # 清理
        try:
            pg_instance.drop_schema(test_schema, cascade=True)
        except:
            pass
        
        # 第一次插入
        pg_instance.insert_pg(sample_dataframe, table_name, overwrite=True)
        original_count = len(pg_instance.query(f"SELECT * FROM {table_name}"))
        
        # 創建新的 DataFrame
        new_df = pd.DataFrame({
            'id': [10, 11],
            'name': ['新員工A', '新員工B'],
            'age': [26, 29],
            'salary': [45000.0, 47000.0],
            'is_active': [True, True],
            'join_date': pd.to_datetime(['2024-01-01', '2024-01-15'])
        })
        
        # 使用覆蓋模式插入新數據
        pg_instance.insert_pg(new_df, table_name, overwrite=True)
        new_count = len(pg_instance.query(f"SELECT * FROM {table_name}"))
        
        assert new_count == len(new_df), "覆蓋後的數據量應該等於新 DataFrame 的行數"
        assert new_count != original_count, "覆蓋後的數據量應該與原始數據量不同"
        
        # 清理
        pg_instance.drop_schema(test_schema, cascade=True)
    
    def test_special_characters_in_names(self, pg_instance):
        """測試名稱中包含特殊字符的處理"""
        # 創建包含特殊字符的 DataFrame
        special_df = pd.DataFrame({
            'normal_col': [1, 2, 3],
            'with space': ['a', 'b', 'c'],
            'with-dash': [1.1, 2.2, 3.3],
            'with.dot': [True, False, True]
        })
        
        test_schema = 'test_special'
        table_name = f'{test_schema}.special_table'
        
        # 清理
        try:
            pg_instance.drop_schema(test_schema, cascade=True)
        except:
            pass
        
        # 插入數據
        pg_instance.insert_pg(special_df, table_name, overwrite=True)
        
        # 查詢數據
        result = pg_instance.query(f'SELECT * FROM {test_schema}."special_table"')
        
        # 驗證所有列都存在
        assert len(result.columns) == len(special_df.columns), "所有列都應該被正確創建"
        assert len(result) == len(special_df), "所有數據都應該被正確插入"
        
        # 清理
        pg_instance.drop_schema(test_schema, cascade=True)


def run_manual_tests():
    """手動運行測試（用於開發時調試）"""
    print("=== 開始手動測試 ===")
    
    pg = PG()
    
    # 創建測試數據
    test_df = pd.DataFrame({
        'employee_id': [1, 2, 3, 4],
        'name': ['Alice Chen', 'Bob Lin', 'Charlie Wang', 'Diana Zhang'],
        'department': ['Engineering', 'Marketing', 'Sales', 'HR'],
        'salary': [75000.50, 65000.00, 55000.75, 60000.25],
        'hire_date': pd.to_datetime(['2022-01-15', '2022-03-20', '2023-06-10', '2023-08-05'])
    })
    
    try:
        print("1. 測試 Schema 創建...")
        pg.create_schema('demo_company')
        print("✓ Schema 創建成功")
        
        print("2. 測試 DataFrame 插入...")
        pg.insert_pg(test_df, 'demo_company.employees', overwrite=True)
        print("✓ DataFrame 插入成功")
        
        print("3. 測試數據查詢...")
        result = pg.query("SELECT * FROM demo_company.employees ORDER BY employee_id")
        print(f"✓ 查詢到 {len(result)} 行數據")
        print(result.head())
        
        print("4. 測試表格描述...")
        table_info = pg.describe_table('demo_company.employees')
        print("✓ 表格結構:")
        print(table_info[['column_name', 'data_type', 'is_nullable']])
        
        print("5. 測試 Schema 列表...")
        schemas = pg.list_schemas()
        print("✓ 可用的 Schemas:")
        print(schemas)
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
    finally:
        # 清理
        try:
            pg.drop_schema('demo_company', cascade=True)
            print("✓ 清理完成")
        except:
            pass
    
    print("=== 手動測試完成 ===")


if __name__ == "__main__":
    run_manual_tests() 
