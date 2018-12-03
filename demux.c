/*demux, por Juan Ureña.
El programa recibira por su entrada estandar
la salida de "cat" sobre un fichero. Además
recibirá como param el numero de bytes que 
se escribirá por cada bloque y vez, y el nombre
de los ficheros en los que se debe esribir. El
programa leera su entrada estandar y la ira 
escribiendo por bloques del tamaño indicado, 
recorriendo la lista de ficheros, como si fuera
circular. */

#include <stdio.h> 
#include <sys/types.h> 
#include <unistd.h> 
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <err.h>
#include <fcntl.h>

enum{
	Bsize = 1024,
};

//Estructura con el estado de mi buffer
struct buffer_information{
	int b_read;//Leidos
	int b_current;//El byte que debe escribir

	int b_block;//Tamaño de mi bloque a escribir
	int b_slopes;//Byte que me queda por escribir del bloque
				//Si es cero, debo escribir el bloque entero
	int son;//Hijo en el que debo escribir
};


int
wait_childs(int num_child)
{
	int status;
	int x;
	int result=0;
	for(x=0;x<num_child;x++){ 
		if (wait(&status)<0){
			warnx("Error en la espera de algun hijo");
			result=1;
		}
		if (WIFEXITED(status) && WEXITSTATUS(status)){
			warnx("Error en un hijo");
			result=1;
		} 
	}
	return(result);
}


void
close_program(int exit_child,int connec[][2],int num_child)
{
	int exit_status=0;
	int i;
	//Cierro todas mis pipes
	for (i=0;i<num_child;i++)
	{
		close(connec[i][1]);
	}
	//Espero todo los hijos
	exit_status=wait_childs(num_child);
	/*Si exit_child es 1, significa que devuelvo el resultado
	que me devuelvan los hijos, si es 0, me dan igual, ha habido
	un error y devolvere fallo.*/
	if (exit_child){
		exit(exit_status);
	}else{
		exit(EXIT_FAILURE);
	}
}


//Codigo del hijo 
void 
son_code(char *name_file, int pipe_child[2])
{
	int waste; 
	int file;

	//Errores a null
	if ((waste=open("/dev/null", O_WRONLY))<0){
		warnx("Error al abrir fichero de salida");
		exit(EXIT_FAILURE);	
	}
	dup2(waste, 2);
	if (close (waste)){
		warnx("Error al cerrar /dev/null");
		exit(EXIT_FAILURE);	
	}


	file = open(name_file, O_CREAT | O_RDWR |O_APPEND| O_TRUNC , 
			S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH );

	if (file<0){
		warnx("Error al abrir fichero de salida");
		exit(EXIT_FAILURE);
	}
	//Entrada sera el pipe, y mi salida el fichero	
	if ((dup2(pipe_child[0],0))<0 || (dup2(file, 1))<0){
		warnx("Error al redireccionar entrada o salida.");
		exit(EXIT_FAILURE);
	}

	if (close(pipe_child[0]) || (close(file))){
		warnx("Error al cerrar descriptor de fichero");
		exit(EXIT_FAILURE);
	}

	execl("/bin/gzip", "gzip", NULL);
}

//Función para generar todos los hijos con sus correspondientes pipes. 
void
generate_childs(int num_childs, char *name_files[], int connec[][2])
{
	int i;
	for (i=0;i<num_childs;i++)
	{
		if((pipe(connec[i]))<0){
			warnx("Error en la espera de algun hijo");
			exit(EXIT_FAILURE);
		}
		switch(fork())
		{
			case 0:
				close(connec[i][1]);
				son_code(name_files[i+2], connec[i]);
				break;
			case -1:
				warnx("Error en el fork");
				exit(EXIT_FAILURE);
			default:
				close(connec[i][0]);
		}
	}
}


struct buffer_information
update_buffer_info(struct buffer_information carac_b)
{
	//Extraigo datos para la comodidad.
	int b_current=carac_b.b_current;
	int b_read=carac_b.b_read;
	int b_slopes=carac_b.b_slopes;
	int b_block=carac_b.b_block;
	int son=carac_b.son;
	int aux;

//Mirar los posibles casos en write_bytes
	if (b_slopes && b_slopes>b_read){
		b_slopes=b_slopes-b_read;//Escribo los pendientes y actualizo los que me quedan
		b_current=b_read;
	}else if(b_slopes && b_slopes<=b_read){
		b_current=b_current+b_slopes;//Avanzo el num de bytes pendientes
		b_slopes=0;//Los pendientes ahora son cero
		son++;	
	}else if((aux=(b_read-b_current))>=b_block){
		b_current=b_current+b_block;//Avanzo mi posicion el tamaño de bloque
		son++;//Cambio de hijo, he escrito todo lo que le corresponde
	}else{
		b_slopes=b_block-b_read+b_current; //Me guardo los que me quedan pendientes
		b_current=b_read; //Mi posicion actual es al final del buffer
	}
	
	//Actualizo los datos
	carac_b.b_slopes=b_slopes;
	carac_b.b_current=b_current;
	carac_b.son=son;

	return carac_b;
}


//Escribe los byte indicados por el usuario
void
write_bytes(struct buffer_information carac_b, int pipe_child[2], char buffer[Bsize])
{
	int b_current=carac_b.b_current;
	int b_read=carac_b.b_read;
	int b_slopes=carac_b.b_slopes;
	int b_block=carac_b.b_block;

	int b_write;


	if (b_slopes && b_slopes>b_read){
		//Tengo bytes pendientes por escribir y son más de los que tengo leidos seguiré teniendo pendientes.
		b_write=b_read;
	}else if(b_slopes && b_slopes<=b_read){
		//Tengo bytes pendientes, pero menos de los que he leido, no tendré pendientes
		b_write=b_slopes;
	}else if((b_read-b_current)>=b_block){
		//Me quedan más bytes por escribir, que el tam del bloque a escribir
		b_write=b_block;	
	}else{
		//No tengo suficientes bytes para escribir, me quedarán pendientes
		b_write=b_read-b_current;
	}

	if ((write(pipe_child[1], &buffer[b_current], b_write))<0){
		warnx("Error en la escritura de la entrada");
		exit(EXIT_FAILURE);
	}
}


//Escribo mi buffer completo
struct buffer_information 
write_buffer(struct buffer_information carac_b, char buffer[Bsize], int connec[][2], int num_child)
{
	carac_b.b_current=0;

	while(carac_b.b_current<=carac_b.b_read-1){
		write_bytes(carac_b,connec[carac_b.son], buffer);

		carac_b=update_buffer_info(carac_b);

		//Compruebo no se haya salido, y reinicio si es así
		//Ya he escrito en el último hijo
		if (carac_b.son==num_child){
			carac_b.son=0;
		}
	}
	carac_b.b_current=0;
	return carac_b;
}


int
main(int argc, char *argv[])
{  
	char buffer[Bsize];
	int num_child=argc-2;
	int connections[num_child][2];

	if (argc<3){
		warnx("Error en la entrada, se debe indicar: ./demux [num_byte_block] [file_out] ...\n Con al menos un fichero de salida");
		exit(EXIT_FAILURE);
	}
	//Inicializo la información de mi buffer
	struct buffer_information my_buffer_info;
	my_buffer_info.b_block=atoi(argv[1]);
	my_buffer_info.b_slopes=0;
	my_buffer_info.son=0; 
	my_buffer_info.b_current=0;

	generate_childs(num_child, argv, connections);

	//Leo de mi entrada estandar hasta leer 0 bytes, en bloques de 1024
	while ((my_buffer_info.b_read=read(0, buffer, Bsize)))
	{
		if (my_buffer_info.b_read<0){
		//Si tengo un valor negativo, significa error en la lectura.
			warnx("Error en la lectura de la entrada");
			close_program(0,connections, num_child);
		}
		my_buffer_info=write_buffer(my_buffer_info, buffer, connections, num_child);
	}

	close_program(1,connections, num_child);
	exit(EXIT_SUCCESS);
}
