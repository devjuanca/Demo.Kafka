using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace KafkaDemo.Api.Migrations;

/// <inheritdoc />
public partial class Init : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "stock_price_snapshot",
            columns: table => new
            {
                Id = table.Column<long>(type: "bigint", nullable: false)
                    .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                Symbol = table.Column<string>(type: "character varying(16)", maxLength: 16, nullable: false),
                Price = table.Column<decimal>(type: "numeric(18,4)", nullable: false),
                Change = table.Column<decimal>(type: "numeric(18,4)", nullable: false),
                ChangePercent = table.Column<decimal>(type: "numeric(18,4)", nullable: false),
                UtcTimestamp = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_stock_price_snapshot", x => x.Id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_stock_price_snapshot_Symbol",
            table: "stock_price_snapshot",
            column: "Symbol");

        migrationBuilder.CreateIndex(
            name: "IX_stock_price_snapshot_UtcTimestamp",
            table: "stock_price_snapshot",
            column: "UtcTimestamp");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "stock_price_snapshot");
    }
}
