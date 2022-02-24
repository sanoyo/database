package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/sanoyo/database"
)

func main() {
	mb := database.NewMemoryBackend()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to database.")

	for {
		fmt.Print("# ")
		text, err := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		fmt.Println("text", text)
		ast, err := database.Parse(text)
		if err != nil {
			panic(err)
		}

		for _, stmt := range ast.Statements {
			switch stmt.Kind {
			case database.CreateTableKind:
				err = mb.CreateTable(ast.Statements[0].CreateTableStatement)
				if err != nil {
					panic(err)
				}
				fmt.Println("ok")
			case database.InsertKind:
				err = mb.Insert(stmt.InsertStatement)
				if err != nil {
					panic(err)
				}

				fmt.Println("ok")
			case database.SelectKind:
				results, err := mb.Select(stmt.SelectStatement)
				if err != nil {
					panic(err)
				}

				for _, col := range results.Columns {
					fmt.Printf("| %s ", col.Name)
				}
				fmt.Println("|")

				for i := 0; i < 20; i++ {
					fmt.Printf("=")
				}
				fmt.Println()

				for _, result := range results.Rows {
					fmt.Printf("|")

					for i, cell := range result {
						typ := results.Columns[i].Type
						s := ""
						switch typ {
						case database.IntType:
							s = fmt.Sprintf("%d", cell.AsInt())
						case database.TextType:
							s = cell.AsText()
						}

						fmt.Printf(" %s | ", s)
					}

					fmt.Println()
				}

				fmt.Println("ok")
			}
		}
	}
}
