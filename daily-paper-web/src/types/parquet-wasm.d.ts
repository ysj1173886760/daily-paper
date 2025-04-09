declare module 'parquet-wasm' {
  export interface Column {
    values: any[];
  }

  export interface Field {
    name: string;
  }

  export interface Schema {
    fields: Field[];
  }

  export interface Table {
    numRows: number;
    schema: Schema;
    getColumn(name: string): Column;
  }

  export function readParquet(buffer: Buffer): Promise<Table>;
} 