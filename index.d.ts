
declare module "fresh-mysql" {

    export default class Db {
        init( options: any, log: any ): void;
        query( sql: string, where?: Array<any>, callback?: ( error: Error|null, data: any ) => void ): void;
        transactionQuery( sqlList: Array< { sql: string, where?: Array<any>, cb?: () => {} } >, callback?: ( error: Error|null, data: any ) => void ): void;
    }

}
